from pandas import to_datetime

def print_formatted_message(d: dict) -> None:
    _print_formatted_message(d, indent=0)

def _print_formatted_message(d: dict, indent: int) -> None:
    for key, value in d.items():
        if isinstance(value, dict):
            _print_formatted_message(value, indent=indent+2)
        else:
            key_name = KEY_NAMES.get(key, key)
            key_name = key_name[0].upper() + key_name[1:]

            if key in VALUE_FORMATTERS:
                formatting_function = VALUE_FORMATTERS.get(key)
                formatted_value = formatting_function(value)
            else:
                formatted_value = value

            string_to_print = ' ' * indent + f'{key_name}: {formatted_value}'
            print(string_to_print)

KEY_NAMES = {
    'e': 'event type',
    'E': 'event time',
    's': 'symbol',
    't': 'trade ID',
    'a': 'aggregate trade ID',
    'p': 'price',
    'q': 'quantity',
    'f': 'first trade ID',
    'l': 'last trade ID',
    'T': 'trade time',
    'm': 'is market maker',
    'M': 'ignore',
}

VALUE_FORMATTERS = {
    'E': lambda x: to_datetime(x, utc=True, unit='ms').strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
    'T': lambda x: to_datetime(x, utc=True, unit='ms').strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
}