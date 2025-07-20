import json
import yaml
import csv

import os
from typing import List, Dict


CONFIG_DIR = "../config/"
DATA_DIR = "../data/"


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
    path = CONFIG_DIR + "broker_config.json"
    with open(path, "r", encoding="utf-8") as f:
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


def load_asset_config(asset: str) -> dict:
    """
    Loads the config for a specific asset from asset_config.yml.
    Returns a dict of settings for the asset.
    Raises a clear error if asset is not found.
    """
    path = CONFIG_DIR + "asset_config.yml"
    with open(path, "r") as f:
        config = yaml.safe_load(f)

    if asset not in config:
        raise ValueError(f"Asset '{asset}' not found in asset_config.yml!")
    return config[asset]


def write_closed_trades(asset, arb_trades, lim_trades):
    filename = DATA_DIR + asset + ".csv"

    # All fields in the Trade class
    fieldnames = [
        "arb_id", "broker", "counter_party", "side", "allowed_slip", "lot_size",
        "entry_price", "sl", "tp", "ticket", "asset", "exit_price", "status",
        "open_time", "close_time", "pnl", "error"
    ]

    # Determine if we need to write the header
    file_exists = os.path.exists(filename)
    write_header = not file_exists or os.path.getsize(filename) == 0

    with open(filename, mode='a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()

        # Write closed arb trades (as 2 rows per tuple)
        for sell, buy in arb_trades:
            for trade in [sell, buy]:
                writer.writerow({f: getattr(trade, f, None) for f in fieldnames})

        # Write closed lim trades (single leg trades)
        for trade in lim_trades:
            writer.writerow({f: getattr(trade, f, None) for f in fieldnames})
