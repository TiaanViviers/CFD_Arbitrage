import json
from typing import List, Dict

CONFIG_PATH = "../config/broker_config.json"


def load_broker_config(asset: str) -> List[Dict]:
    """
    Load and validate the broker configuration from a JSON file.

    Args:
        None

    Returns:
        A list of validated broker configuration dictionaries.

    Raises:
        ValueError: If required fields are missing or malformed.
        FileNotFoundError: If the config file is missing.
        json.JSONDecodeError: If the file isn't valid JSON.
    """
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)

    valid_types = {"crypto", "index.us", "index.eu", "index.as"}
    asset = asset.upper()  # Just in case

    filtered_brokers = []

    for entry in config:
        # Validation as before
        if "broker" not in entry:
            raise ValueError("Missing 'broker' in config entry")
        if "terminal_path" not in entry:
            raise ValueError("Missing 'terminal_path' in config entry")
        if "symbols" not in entry or not isinstance(entry["symbols"], list):
            raise ValueError("Missing or invalid 'symbols' list in config entry")

        # Filter symbols for this broker
        matching_symbols = [
            s for s in entry["symbols"]
            if s.get("internal", "").upper() == asset
        ]

        for symbol in matching_symbols:
            if "broker_symbol" not in symbol:
                raise ValueError("Missing 'broker_symbol' in symbol entry")
            if "type" not in symbol:
                raise ValueError(f"Symbol {symbol['internal']} is missing 'type'")
            if symbol["type"] not in valid_types:
                raise ValueError(f"Symbol {symbol['internal']} has invalid type '{symbol['type']}'")

        # Only include this broker if they have the requested asset
        if matching_symbols:
            filtered_brokers.append({
                "broker": entry["broker"],
                "terminal_path": entry["terminal_path"],
                "symbols": matching_symbols
            })

    return filtered_brokers

