import os
import json
import yaml
import csv
from typing import Any, List, Dict

# --- Constants ---
CONFIG_DIR = os.path.join("..", "config")
DATA_DIR = os.path.join("..", "data")
BROKER_CONFIG = os.path.join(CONFIG_DIR, "broker_config.json")
ASSET_CONFIG = os.path.join(CONFIG_DIR, "asset_config.yml")

TRADE_FIELDNAMES = [
    "arb_id", "broker", "counter_party", "side", "allowed_slip", "lot_size",
    "entry_price", "sl", "tp", "ticket", "asset", "exit_price", "status",
    "open_time", "close_time", "pnl", "error"
]

VALID_TYPES = {"crypto", "index.us", "index.eu", "index.as"}


def load_broker_config(asset: str) -> List[Dict[str, Any]]:
    """Load and validate broker configuration for the given asset.

    Args:
        asset: Asset symbol (e.g., 'BTCUSD').

    Returns:
        List of broker configurations matching the asset.

    Raises:
        ValueError: On missing/malformed fields.
        FileNotFoundError: If the config file is missing.
        json.JSONDecodeError: If the file isn't valid JSON.
    """
    asset = asset.upper()
    with open(BROKER_CONFIG, "r", encoding="utf-8") as f:
        config = json.load(f)

    filtered = []
    for entry in config:
        if "broker" not in entry:
            raise ValueError("Missing 'broker' in config entry")
        if "terminal_path" not in entry:
            raise ValueError("Missing 'terminal_path' in config entry")
        if "symbols" not in entry or not isinstance(entry["symbols"], list):
            raise ValueError("Missing or invalid 'symbols' list in config entry")

        matches = [
            s for s in entry["symbols"]
            if s.get("internal", "").upper() == asset
        ]
        for symbol in matches:
            if "broker_symbol" not in symbol:
                raise ValueError("Missing 'broker_symbol' in symbol entry")
            if "type" not in symbol:
                raise ValueError(f"Symbol {symbol.get('internal', '')} is missing 'type'")
            if symbol["type"] not in VALID_TYPES:
                raise ValueError(
                    f"Symbol {symbol.get('internal', '')} has invalid type '{symbol['type']}'"
                )
        if matches:
            filtered.append({
                "broker": entry["broker"],
                "terminal_path": entry["terminal_path"],
                "symbols": matches
            })

    return filtered


def load_asset_config(asset: str) -> dict[str, Any]:
    """Load config for a specific asset from asset_config.yml.

    Args:
        asset: Asset symbol to load config for.

    Returns:
        Dict of asset config settings.

    Raises:
        ValueError: If asset not found in config.
    """
    with open(ASSET_CONFIG, "r") as f:
        config = yaml.safe_load(f)
    if asset not in config:
        raise ValueError(f"Asset '{asset}' not found in asset_config.yml!")
    return config[asset]


def write_closed_trades(asset: str, arb_trades: List[tuple], lim_trades: List[Any]) -> None:
    """Append closed trades to the CSV file for the given asset.

    Args:
        asset: Asset symbol (used as filename).
        arb_trades: List of (sell, buy) tuples.
        lim_trades: List of single-leg trades.
    """
    filename = os.path.join(DATA_DIR, f"{asset}.csv")
    file_exists = os.path.exists(filename)
    write_header = not file_exists or os.path.getsize(filename) == 0

    with open(filename, mode='a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=TRADE_FIELDNAMES)
        if write_header:
            writer.writeheader()

        # Write arb trades (each tuple = (sell, buy))
        for sell, buy in arb_trades:
            for trade in (sell, buy):
                writer.writerow({f: getattr(trade, f, None) for f in TRADE_FIELDNAMES})

        # Write lim trades (single-leg)
        for trade in lim_trades:
            writer.writerow({f: getattr(trade, f, None) for f in TRADE_FIELDNAMES})
