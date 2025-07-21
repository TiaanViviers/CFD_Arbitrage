"""
MT5BrokerInterface: Thin wrapper around MetaTrader 5 Python API.

Handles connection, symbol selection, order placement, position management,
and market status checks. Used by worker processes for isolated broker control.
All methods are thread-safe and log key events for auditability.
"""

import MetaTrader5 as mt5
import threading
from datetime import datetime, timedelta, UTC
import time

################################ Constants / Config ################################

FILLING_TYPE_MAP = {
    "icmarkets": mt5.ORDER_FILLING_IOC,
    "exness":    mt5.ORDER_FILLING_IOC,
    "fxtm":      mt5.ORDER_FILLING_FOK,
    "eightcap":  mt5.ORDER_FILLING_IOC,
    "xm":        mt5.ORDER_FILLING_IOC,
}


class MT5BrokerInterface:
    def __init__(self, name: str, path: str, symbol: str, logger):
        self.name = name
        self.path = path
        self.symbol = symbol
        self.logger = logger
        self.digits = None
        self.filling_type = None
        self.blocked_until = None
        self._lock = threading.RLock()
        self.connect()


    ############################ Connection & Setup ############################
    def connect(self, max_attempts: int = 10) -> bool:
        """
        Connect to MT5 terminal and subscribe to symbol, with retries.
        """
        attempts = 0
        while attempts < max_attempts:
            with self._lock:
                if mt5.initialize(self.path):
                    self.logger.info(f"[{self.name}] Connected to MT5 terminal.")
                    if not mt5.symbol_select(self.symbol, True):
                        self.logger.warning(
                            f"[{self.name}] Could not subscribe to symbol {self.symbol}"
                        )
                    else:
                        self.get_digits()
                        self.get_filling_type()
                    return True
                else:
                    e = mt5.last_error()
                    self.logger.warning(
                        f"[{self.name}] MT5 init failed, Attempt {attempts+1}: {e}"
                    )
            attempts += 1
            time.sleep(2 * (2 ** min(attempts - 1, 3)))
        self.logger.error(
            f"[{self.name}] Failed to connect after {attempts} attempts."
        )
        return False


    def shutdown(self) -> None:
        """
        Gracefully disconnect from MT5 terminal.
        """
        with self._lock:
            try:
                mt5.shutdown()
                self.logger.info(f"[{self.name}] Disconnected from MT5 terminal.")
            except Exception as e:
                self.logger.error(
                    f"[{self.name}] Error during shutdown: {e}"
                )


    def get_digits(self) -> None:
        """
        Query the number of decimals for this symbol.
        """
        info = mt5.symbol_info(self.symbol)
        if info is None:
            self.logger.warning(
                f"[{self.name}] Could not get symbol info for {self.symbol}. Defaulting digits to 2."
            )
            self.digits = 2
        else:
            self.digits = info.digits


    def get_filling_type(self) -> None:
        """
        Set order filling type from broker name (defaults to IOC).
        """
        self.filling_type = FILLING_TYPE_MAP.get(
            self.name, mt5.ORDER_FILLING_IOC
        )


    ########################## Market State & Query ############################
    def get_latest_tick(self):
        """
        Return latest tick dict or None if market closed or invalid.
        """
        with self._lock:
            try:
                if not self.order_check_ping() or self._is_in_timeout():
                    return None
                tick = mt5.symbol_info_tick(self.symbol)
                if tick is None or tick.bid <= 0 or tick.ask <= 0:
                    self.logger.warning(
                        f"[{self.name}] Dirty tick for {self.symbol}"
                    )
                    return None
                return {
                    "timestamp": datetime.now(UTC).isoformat(),
                    "bid": tick.bid,
                    "ask": tick.ask,
                }
            except Exception as e:
                self.logger.error(
                    f"[{self.name}] Error fetching tick: {e}"
                )
                return None


    def order_check_ping(self) -> bool:
        """
        Simulate a tiny buy order to test if the market is open.
        """
        with self._lock:
            info = mt5.symbol_info(self.symbol)
            if info is None or info.ask <= 0:
                return False
            req = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": self.symbol,
                "volume": 0.1,
                "type": mt5.ORDER_TYPE_BUY,
                "price": info.ask,
                "deviation": 300,
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": self.filling_type,
            }
            res = mt5.order_check(req)
            return res is not None and res.retcode == 0


    def get_balance(self, retries: int = 2, retry_delay: float = 0.05):
        """
        Return account balance, retrying if needed.
        """
        with self._lock:
            for attempt in range(retries + 1):
                try:
                    account_info = mt5.account_info()
                    if account_info is not None:
                        return account_info.balance
                    elif attempt < retries:
                        time.sleep(retry_delay)
                except Exception as e:
                    self.logger.error(
                        f"[{self.name}] Exception getting balance: {e}"
                    )
                    if attempt < retries:
                        time.sleep(retry_delay)
            self.logger.error(
                f"[{self.name}] Could not get balance after {retries+1} attempts."
            )
            return None
    

    def get_positions(self):
        """
        Return all positions for this symbol (raw MT5 objects).
        """
        return mt5.positions_get(symbol=self.symbol)
    

    def get_open_positions(self):
        """
        Return list of open positions (dicts) for this symbol.
        """
        with self._lock:
            try:
                positions = mt5.positions_get(symbol=self.symbol)
                if positions is None:
                    self.logger.warning(
                        f"[{self.name}] Could not fetch open positions for {self.symbol}."
                    )
                    return []
                return [
                    {
                        "ticket": pos.ticket,
                        "magic": pos.magic,
                        "side": "buy" if pos.type == mt5.ORDER_TYPE_BUY else "sell",
                        "volume": pos.volume,
                    }
                    for pos in positions
                ]
            except Exception as e:
                self.logger.error(
                    f"[{self.name}] Exception in get_open_positions: {e}"
                )
                return []
            
    
    def set_timeout(self, seconds: int = 1800) -> None:
        """
        Block this broker from tick/trading for the next `seconds`.
        """
        self.blocked_until = datetime.now(UTC) + timedelta(seconds=seconds)
        self.logger.warning(
            f"[{self.name}] Broker timed out until {self.blocked_until.isoformat()}."
        )


    ############################# Order Management #############################
    def place_order(self, side, lots, price=None, sl=None, tp=None,
                    deviation=200, magic=1000, comment=''):
        """
        Place an order for the given side, lots, optional price/sl/tp.
        Returns the result object or None.
        """
        with self._lock:
            type_map = {'buy': mt5.ORDER_TYPE_BUY, 'sell': mt5.ORDER_TYPE_SELL}
            if side not in type_map:
                self.logger.error(
                    f"[{self.name}] Invalid order side: {side} (must be buy/sell)"
                )
                return None

            try:
                tick = mt5.symbol_info_tick(self.symbol)
                if tick is None:
                    self.logger.error(
                        f"[{self.name}] No tick for {self.symbol}."
                    )
                    return None

                exec_price = price
                if exec_price is None:
                    exec_price = self._get_market_price_for_side(side, tick)
                    if exec_price is None:
                        self.logger.error(
                            f"[{self.name}] Invalid tick: bid={tick.bid}, ask={tick.ask}"
                        )
                        return None

                req = self._build_order_request(
                    side, lots, exec_price, sl, tp, deviation, magic, comment
                )

                result = mt5.order_send(req)

                if result is None:
                    self.logger.error(
                        f"[{self.name}] order_send() returned None for {self.symbol}."
                    )
                    self.logger.error(
                        f"[{self.name}] order_send failed: {mt5.last_error()}"
                    )
                    return None

                if result.retcode != mt5.TRADE_RETCODE_DONE:
                    self.logger.error(
                        f"[{self.name}] Order failed for {self.symbol}: "
                        f"{result.comment} (retcode={result.retcode})"
                    )
                    return result

                self.logger.info(
                    f"[{self.name}] Order placed: {side} {lots} {self.symbol} "
                    f"@ {exec_price} (ticket: {result.order})"
                )
                return result

            except Exception as e:
                self.logger.error(
                    f"[{self.name}] Exception during order: {e}",
                    exc_info=True
                )
                return None


    def close_position(self, ticket=None, volume=None,
                       deviation=200, magic=1000, comment=''):
        """
        Close open position(s) for this symbol.
        If `ticket` is given, closes only that position.
        Returns the result object or None.
        """
        with self._lock:
            try:
                positions = mt5.positions_get(symbol=self.symbol)
                if not positions:
                    self.logger.info(
                        f"[{self.name}] No open positions to close for {self.symbol}."
                    )
                    return None

                for pos in positions:
                    if ticket is not None and pos.ticket != ticket:
                        continue
                    close_type, price = self._get_close_type_and_price(pos)
                    if price is None:
                        continue

                    close_vol = volume if volume is not None else pos.volume
                    req = self._build_close_request(
                        pos, close_type, close_vol, price, deviation, magic, comment
                    )

                    self.logger.info(
                        f"[{self.name}] Closing ticket {pos.ticket}: "
                        f"{close_type} {close_vol} {self.symbol} @ {price}"
                    )
                    result = mt5.order_send(req)
                    if result is None:
                        self.logger.error(
                            f"[{self.name}] order_send() returned None while closing {pos.ticket} on {self.symbol}."
                        )
                        return None
                    if result.retcode != mt5.TRADE_RETCODE_DONE:
                        self.logger.error(
                            f"[{self.name}] Failed to close {pos.ticket}: "
                            f"{result.comment} (retcode={result.retcode})"
                        )
                        return None
                    self.logger.info(
                        f"[{self.name}] Closed ticket {pos.ticket}: "
                        f"{close_type} {close_vol} {self.symbol} (order: {result.order})"
                    )
                    return result

            except Exception as e:
                self.logger.error(
                    f"[{self.name}] Exception while closing position: {e}",
                    exc_info=True
                )
                return None


    ############################# Private Helpers ##############################
    def _get_market_price_for_side(self, side, tick):
        """
        Given 'buy' or 'sell' and a tick, return the correct price or None.
        """
        if side == 'buy' and tick.ask > 0:
            return tick.ask
        if side == 'sell' and tick.bid > 0:
            return tick.bid
        return None


    def _build_order_request(
        self, side, lots, price, sl, tp, deviation, magic, comment
    ):
        """
        Compose the dict for order_send().
        """
        type_map = {'buy': mt5.ORDER_TYPE_BUY, 'sell': mt5.ORDER_TYPE_SELL}
        req = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": self.symbol,
            "volume": lots,
            "type": type_map[side],
            "price": price,
            "deviation": deviation,
            "magic": magic,
            "comment": comment,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": self.filling_type,
        }
        if tp is not None and sl is not None:
            req["tp"] = tp
            req["sl"] = sl
        return req


    def _get_close_type_and_price(self, pos):
        """
        Return (close_type, price) tuple for a given MT5 position.
        """
        if pos.type == mt5.POSITION_TYPE_BUY:
            close_type = mt5.ORDER_TYPE_SELL
            tick = mt5.symbol_info_tick(self.symbol)
            if tick is None or tick.bid <= 0:
                self.logger.error(
                    f"[{self.name}] No valid bid price to close BUY {pos.ticket}."
                )
                return close_type, None
            return close_type, tick.bid
        elif pos.type == mt5.POSITION_TYPE_SELL:
            close_type = mt5.ORDER_TYPE_BUY
            tick = mt5.symbol_info_tick(self.symbol)
            if tick is None or tick.ask <= 0:
                self.logger.error(
                    f"[{self.name}] No valid ask price to close SELL {pos.ticket}."
                )
                return close_type, None
            return close_type, tick.ask
        else:
            self.logger.error(
                f"[{self.name}] Unknown position type for ticket {pos.ticket}."
            )
            return None, None


    def _build_close_request(
        self, pos, close_type, close_vol, price, deviation, magic, comment
    ):
        """
        Compose the dict for closing position (order_send()).
        """
        return {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": self.symbol,
            "volume": close_vol,
            "type": close_type,
            "position": pos.ticket,
            "price": price,
            "deviation": deviation,
            "magic": magic,
            "comment": comment,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": self.filling_type,
        }
    

    def _is_in_timeout(self) -> bool:
        """
        True if broker is currently blocked.
        """
        return (
            self.blocked_until is not None
            and datetime.now(UTC) < self.blocked_until
        )
    

    def __repr__(self):
        return (
            f"<MT5BrokerInterface("
            f"name='{self.name}', "
            f"path='{self.path}', "
            f"symbol='{self.symbol}', "
            f"logger='{self.logger.name}'"
            f")>"
        )
