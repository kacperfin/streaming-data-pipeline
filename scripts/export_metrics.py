"""
Metrics exporter for Prometheus data.

This script exports metrics from Prometheus for a specified time range and outputs them
as a unified CSV file. All metrics are aggregated in PromQL (e.g., sum across partitions)
to simplify analysis.

Workflow:
1. Run your pipeline continuously
2. Note the start/end times of your experiment
3. Run this script with those timestamps to export metrics to CSV
4. Analyze the CSV in Pandas/Jupyter

Example:
    # Export metrics for a 2-minute experiment
    python export_metrics.py \\
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
from typing import Dict, Optional

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

    def query_range(self, query: str, start: float, end: float, step: str = "5s") -> Dict:
        """
        Query Prometheus for a time range.

        Args:
            query: PromQL query string
            start: Start time (Unix timestamp)
            end: End time (Unix timestamp)
            step: Resolution - should match scrape_interval (default: '5s')

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


def export_metrics(start_time: str, end_time: str, output_dir: Path, experiment_name: str, step: str = "5s"):
    """
    Export calculated metrics from Prometheus for a specific time range.
    Creates a single unified CSV with all metrics as columns.

    Args:
        start_time: Start time (ISO format: "2025-12-10T15:05:00")
        end_time: End time (ISO format: "2025-12-10T15:07:00")
        output_dir: Directory to save results
        experiment_name: Name/ID for this experiment
        step: Query resolution (default: "5s" to match scrape_interval)

    Note:
        PromQL queries in metrics_definitions.json must return a single time series. If a query returns multiple
        series (e.g., using "sum by (label)" which returns one series per label value),
        values will overwrite each other and only the last series will be kept.
        Use aggregations like "sum()" to ensure a single series per metric.
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

    logger.info(f"Querying {len(exporter.metrics)} metrics from Prometheus...")

    # Collect all metrics data
    all_metrics_data = {}  # {timestamp: {metric_name: value, ...}}

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

            if result['status'] != 'success':
                logger.error(f"Query failed for {metric_name}: {result}")
                continue

            data = result['data']['result']

            if not data:
                logger.warning(f"No data returned for metric: {metric_name}")
                continue

            # Extract metric values
            # Each metric should return a single series (aggregated in PromQL if needed)
            for series in data:
                for timestamp, value in series['values']:
                    ts_iso = datetime.fromtimestamp(timestamp).isoformat()

                    if ts_iso not in all_metrics_data:
                        all_metrics_data[ts_iso] = {}

                    all_metrics_data[ts_iso][metric_name] = value

        except Exception as e:
            logger.error(f"Failed to query {metric_name}: {e}")

    # Write unified CSV
    if not all_metrics_data:
        logger.error("No metrics data collected!")
        return

    logger.info(f"Writing unified CSV with {len(all_metrics_data)} timestamps...")

    # Determine all column names
    all_columns = set()
    for timestamp_data in all_metrics_data.values():
        all_columns.update(timestamp_data.keys())

    sorted_columns = sorted(all_columns)

    # Write to CSV
    output_file = output_dir / f"{experiment_name}.csv"

    with open(output_file, 'w', newline='') as csvfile:
        fieldnames = ['timestamp'] + sorted_columns
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for timestamp in sorted(all_metrics_data.keys()):
            row = {'timestamp': timestamp}
            for column in sorted_columns:
                row[column] = all_metrics_data[timestamp].get(column, '')
            writer.writerow(row)

    logger.info(f"Unified CSV exported: {output_file}")

    # Save experiment metadata
    metadata = {
        'experiment_name': experiment_name,
        'start_time': start_time,
        'end_time': end_time,
        'duration_seconds': duration,
        'step': step,
        'metrics_exported': list(exporter.metrics.keys()),
        'columns': sorted_columns,
        'num_timestamps': len(all_metrics_data)
    }

    metadata_file = output_dir / f"{experiment_name}_metadata.json"
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)

    logger.info(f"Export complete! Results saved to: {output_dir}")
    logger.info(f"Metadata saved to: {metadata_file}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Export metrics from Prometheus to CSV for a specified time range',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Export metrics for a 2-minute experiment
  python export_metrics.py \\
      --start "2025-12-10T15:05:00" \\
      --end "2025-12-10T15:07:00" \\
      --name "onpremise_5symbols"

  # Use custom step (e.g., 30 seconds instead of default 5)
  python export_metrics.py \\
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
        default='5s',
        help='Query resolution - should match Prometheus scrape_interval (default: 5s)'
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
