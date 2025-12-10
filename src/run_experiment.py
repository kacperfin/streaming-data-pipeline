"""
Experiment runner for collecting metrics from Prometheus.

This script exports calculated metrics (throughput, latency, error rates) from Prometheus
for a specified time range. Use this to collect data for on-premise vs AWS comparison.

Workflow:
1. Run your pipeline continuously
2. Note the start/end times of your experiment
3. Run this script with those timestamps to export metrics
4. Analyze the CSV files in Pandas

Example:
    # Export metrics for a 2-minute experiment
    python run_experiment.py \\
        --start "2025-12-10T15:05:00" \\
        --end "2025-12-10T15:07:00" \\
        --name "onpremise_5symbols"
"""

import argparse
import csv
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('experiment_runner')


class PrometheusExporter:
    """Exports calculated metrics from Prometheus to CSV."""

    def __init__(self, prometheus_url: str = "http://localhost:9090", metrics_file: Optional[Path] = None):
        """
        Initialize the exporter.

        Args:
            prometheus_url: Base URL for Prometheus server
            metrics_file: Path to metrics definitions JSON file
        """
        self.prometheus_url = prometheus_url
        self.api_url = f"{prometheus_url}/api/v1"
        self.metrics = self._load_metrics(metrics_file)

    def _load_metrics(self, metrics_file: Optional[Path] = None) -> Dict[str, str]:
        """
        Load metrics definitions from JSON file.

        Args:
            metrics_file: Path to metrics JSON file (defaults to ../metrics_definitions.json)

        Returns:
            Dict mapping metric names to PromQL queries
        """
        if metrics_file is None:
            # Default: look for metrics_definitions.json in project root
            metrics_file = Path(__file__).parent.parent / "metrics_definitions.json"

        try:
            with open(metrics_file, 'r') as f:
                definitions = json.load(f)

            # Flatten nested structure: component_metric_name -> query
            metrics = {}
            for component, component_metrics in definitions.items():
                for metric_name, metric_config in component_metrics.items():
                    full_name = f"{component}_{metric_name}"
                    metrics[full_name] = metric_config['query']

            logger.info(f"Loaded {len(metrics)} metrics from {metrics_file}")
            return metrics

        except FileNotFoundError:
            logger.error(f"Metrics file not found: {metrics_file}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in metrics file: {e}")
            raise

    def query_range(self, query: str, start: float, end: float, step: str = "15s") -> Dict:
        """
        Query Prometheus for a time range.

        Args:
            query: PromQL query string
            start: Start time (Unix timestamp)
            end: End time (Unix timestamp)
            step: Resolution - should match scrape_interval (default: '15s')

        Returns:
            Dict containing the query result
        """
        params = {
            'query': query,
            'start': start,
            'end': end,
            'step': step
        }

        try:
            response = requests.get(f"{self.api_url}/query_range", params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to query Prometheus: {e}")
            raise

    def export_to_csv(self, metric_name: str, result: Dict, output_file: Path):
        """
        Export Prometheus query result to CSV.

        Args:
            metric_name: Name of the metric being exported
            result: Prometheus query result (JSON)
            output_file: Path to output CSV file
        """
        if result['status'] != 'success':
            logger.error(f"Query failed: {result}")
            return

        data = result['data']['result']

        if not data:
            logger.warning(f"No data returned for metric: {metric_name}")
            return

        # Open CSV file for writing
        with open(output_file, 'w', newline='') as csvfile:
            # Create header: timestamp, value, and all label keys
            all_labels = set()
            for series in data:
                all_labels.update(series['metric'].keys())

            fieldnames = ['timestamp', 'value'] + sorted(all_labels)
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            # Write data rows
            for series in data:
                labels = series['metric']

                # Each series has multiple time-value pairs
                for timestamp, value in series['values']:
                    row = {
                        'timestamp': datetime.fromtimestamp(timestamp).isoformat(),
                        'value': value,
                        **labels  # Unpack all labels
                    }
                    writer.writerow(row)

        logger.info(f"Exported {metric_name} to {output_file}")


def export_metrics(start_time: str, end_time: str, output_dir: Path, experiment_name: str, step: str = "15s"):
    """
    Export calculated metrics from Prometheus for a specific time range.

    Args:
        start_time: Start time (ISO format: "2025-12-10T15:05:00")
        end_time: End time (ISO format: "2025-12-10T15:07:00")
        output_dir: Directory to save results
        experiment_name: Name/ID for this experiment
        step: Query resolution (default: "15s" to match scrape_interval)
    """
    logger.info(f"Exporting metrics for experiment: {experiment_name}")
    logger.info(f"Time range: {start_time} to {end_time}")
    logger.info(f"Step: {step}")

    # Parse timestamps
    start_dt = datetime.fromisoformat(start_time)
    end_dt = datetime.fromisoformat(end_time)
    start_unix = start_dt.timestamp()
    end_unix = end_dt.timestamp()

    duration = end_unix - start_unix
    logger.info(f"Duration: {duration} seconds ({duration/60:.1f} minutes)")

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Initialize Prometheus exporter
    exporter = PrometheusExporter()

    logger.info(f"Exporting {len(exporter.metrics)} calculated metrics...")

    # Export each metric
    for metric_name, promql_query in exporter.metrics.items():
        try:
            logger.info(f"Querying: {metric_name}")
            logger.debug(f"  PromQL: {promql_query}")

            result = exporter.query_range(
                query=promql_query,
                start=start_unix,
                end=end_unix,
                step=step
            )

            # Create filename
            output_file = output_dir / f"{experiment_name}_{metric_name}.csv"

            # Export to CSV
            exporter.export_to_csv(metric_name, result, output_file)

        except Exception as e:
            logger.error(f"Failed to export {metric_name}: {e}")

    # Save experiment metadata
    metadata = {
        'experiment_name': experiment_name,
        'start_time': start_time,
        'end_time': end_time,
        'duration_seconds': duration,
        'step': step,
        'metrics_exported': list(exporter.metrics.keys())
    }

    metadata_file = output_dir / f"{experiment_name}_metadata.json"
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)

    logger.info(f"Export complete! Results saved to: {output_dir}")
    logger.info(f"Metadata saved to: {metadata_file}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Export calculated metrics from Prometheus for a time range',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Export metrics for a 2-minute experiment
  python run_experiment.py \\
      --start "2025-12-10T15:05:00" \\
      --end "2025-12-10T15:07:00" \\
      --name "onpremise_5symbols"

  # Use custom step (e.g., 30 seconds instead of default 15)
  python run_experiment.py \\
      --start "2025-12-10T15:00:00" \\
      --end "2025-12-10T16:00:00" \\
      --name "onpremise_long_test" \\
      --step "30s"
        """
    )
    parser.add_argument(
        '--start',
        type=str,
        required=True,
        help='Experiment start time (ISO format: "2025-12-10T15:05:00")'
    )
    parser.add_argument(
        '--end',
        type=str,
        required=True,
        help='Experiment end time (ISO format: "2025-12-10T15:07:00")'
    )
    parser.add_argument(
        '--name',
        type=str,
        required=True,
        help='Experiment name (e.g., "onpremise_5symbols", "aws_10symbols")'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='experiments',
        help='Output directory for results (default: experiments/)'
    )
    parser.add_argument(
        '--step',
        type=str,
        default='15s',
        help='Query resolution - should match Prometheus scrape_interval (default: 15s)'
    )

    args = parser.parse_args()

    output_dir = Path(args.output_dir)

    export_metrics(
        start_time=args.start,
        end_time=args.end,
        output_dir=output_dir,
        experiment_name=args.name,
        step=args.step
    )


if __name__ == '__main__':
    main()
