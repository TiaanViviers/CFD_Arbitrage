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
import math

################################ Constants / Config ################################

FILLING_TYPE_MAP = {
    "icmarkets": mt5.ORDER_FILLING_IOC,
    "exness":    mt5.ORDER_FILLING_IOC,
    "fxtm":      mt5.ORDER_FILLING_FOK,
    "eightcap":  mt5.ORDER_FILLING_IOC,
    "xm":        mt5.ORDER_FILLING_IOC,
}

TYPE_MAP = {'buy': mt5.ORDER_TYPE_BUY, 'sell': mt5.ORDER_TYPE_SELL}


class MT5BrokerInterface:
    def __init__(self, name: str, path: str, symbol: str, logger):
        self.name = name
        self.path = path
        self.symbol = symbol
        self.logger = logger
        self.digits = None
        self.contract_size = None
        self.min_lot = None
        self.leverage = None
        self.filling_type = None
        self.blocked_until = None
        self._last_price = None
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
                        self._get_filling_type()
                        sym_info = self._get_symbol_info()
                        self._set_symbol_info(sym_info)
                        acc_info = self._get_account_info()
                        self._set_account_info(acc_info)
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


    ########################## Market State & Query ############################
    def get_latest_tick(self):
        """
        Return latest tick dict or None if market closed or invalid.
        """
        with self._lock:
            try:
                if self._is_in_timeout():
                    return None
                tick = mt5.symbol_info_tick(self.symbol)
                if tick is None or tick.bid <= 0 or tick.ask <= 0:
                    self.logger.warning(
                        f"[{self.name}] Dirty tick for {self.symbol}"
                    )
                    return None
                
                self._last_price = {"bid": tick.bid, "ask": tick.ask}
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


    def get_capital_state(self, side: str = "buy") -> dict:
        """
        Return current account balance and max lot size for given side.
        Uses cached last price to calculate max lot.
        """
        info = mt5.account_info()
        if not info:
            return {"balance": 0.0, "max_lot": 0.0}

        max_lot = self._calc_max_lot(info.margin_free, side)
        return {
            "balance": info.balance,
            "max_lot": max_lot
        }


    def get_trade_profit(self, deal_ticket: int) -> float:
        """
        Return broker-reported profit (USD) for a specific deal ticket.
        If the deal is not found, returns None.
        """
        if deal_ticket <= 0:
            return None
        try:
            deals = mt5.history_deals_get(ticket=deal_ticket)
            if deals and len(deals) > 0 and hasattr(deals[0], "profit"):
                return deals[0].profit
        except Exception as e:
            self.logger.error(f"[{self.name}] history_deal_get failed: {e}")
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
            
            if side not in TYPE_MAP:
                self.logger.error(
                    f"[{self.name}] Invalid order side: {side} (must be buy/sell)"
                )
                return None

            try:
                exec_price = price
                if exec_price is None:
                    exec_price = self._get_market_price_for_side(side)
                    if exec_price is None:
                        self.logger.error(
                            f"[{self.name}] Invalid tick: no price for {side}."
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


    def update_sl(self, ticket, magic, new_sl):
        """
        Update stop loss for an existing position. 
        """
        request = {
                    "action": mt5.TRADE_ACTION_SLTP,
                    "symbol": self.symbol,
                    "position": ticket,
                    "sl": new_sl,
                    "magic": magic,
                    "type_time": mt5.ORDER_TIME_GTC,
                    "type_filling": self.filling_type
                }
        result = mt5.order_send(request)
        if result is None:
            self.logger.error(f"[{self.name}] order_send returned None while updating SL")
            return False
        
        if result.retcode != mt5.TRADE_RETCODE_DONE:
            self.logger.error(f"[{self.name}] Failed to update SL (retcode={result.retcode}): {result.comment}")
            return False
        
        self.logger.info(f"[{self.name}] SL updated to {new_sl} for {self.symbol}")
        return True


    ############################# Private Helpers ##############################
    def _get_market_price_for_side(self, side):
        """
        Given 'buy' or 'sell' and a tick, return the correct price or None.
        """
        if side == 'buy' and self._last_price.ask > 0:
            return self._last_price.ask
        if side == 'sell' and self._last_price.bid > 0:
            return self._last_price.bid
        return None


    def _build_order_request(
        self, side, lots, price, sl, tp, deviation, magic, comment
    ):
        """
        Compose the dict for order_send().
        """
        req = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": self.symbol,
            "volume": lots,
            "type": TYPE_MAP[side],
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
            price      = getattr(self, "_last_price", {}).get("bid", 0)
            if price <= 0:
                tick  = mt5.symbol_info_tick(self.symbol)
                price = tick.bid if tick else 0
        elif pos.type == mt5.POSITION_TYPE_SELL:
            close_type = mt5.ORDER_TYPE_BUY
            price      = getattr(self, "_last_price", {}).get("ask", 0)
            if price <= 0:
                tick  = mt5.symbol_info_tick(self.symbol)
                price = tick.ask if tick else 0
        else:
            self.logger.error(f"[{self.name}] Unknown position type {pos.ticket}")
            return None, None

        if price <= 0:
            self.logger.error(f"[{self.name}] No valid price to close {pos.ticket}")
            return close_type, None

        return close_type, price


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
    

    def _calc_max_lot(self, free_margin: float, side: str = "buy") -> float:
        """
        Convert current free-margin into the largest lot the broker will accept.
        Uses cached last price – never queries MT5 again.
        """
        if free_margin <= 0 or not getattr(self, "_last_price", None):
            return 0.0

        if self._last_price == None:
            return 0.0
        price = (self._last_price["ask"] if side == "buy"
                else self._last_price["bid"])
        if price <= 0:
            return 0.0

        margin_per_lot = self.contract_size * price / self.leverage
        if margin_per_lot <= 0:
            return 0.0

        raw_lot = free_margin / margin_per_lot
        step    = self.min_lot
        lot     = math.floor(raw_lot / step) * step
        return round(max(lot, self.min_lot), 2)


    def _is_in_timeout(self) -> bool:
        """
        True if broker is currently blocked.
        """
        return (
            self.blocked_until is not None
            and datetime.now(UTC) < self.blocked_until
        )
    

    def _get_symbol_info(self, max_attempts: int = 5, base_delay: float = 0.5):
        """
        Fetch symbol_info with retries and exponential back-off.

        Returns:
            mt5.symbol_info() object or None if not retrieved.
        """
        for attempt in range(1, max_attempts + 1):
            info = mt5.symbol_info(self.symbol)
            if info:
                return info

            self.logger.warning(
                f"[{self.name}] Unable to fetch symbol info for {self.symbol} "
                f"(attempt {attempt}/{max_attempts})"
            )
            # exponential back-off
            time.sleep(base_delay * (2 ** (attempt - 1)))

        self.logger.error(
            f"[{self.name}] Failed to obtain symbol info for {self.symbol} "
            f"after {max_attempts} attempts."
        )
        return None
    

    def _set_symbol_info(self, info):
        """ Set symbol info attributes based on retrieved data."""
        if info is None:
            self.logger.warning("Symbol info is None, going to default settings.")
            self.digits        = 2
            self.contract_size = 1.0
            self.min_lot       = 0.01
        else:
            self.digits        = info.digits
            self.contract_size = info.trade_contract_size
            self.min_lot       = info.volume_min


    def _get_account_info(self, max_attempts: int = 5, base_delay: float = 0.5):
        """
        Fetch account_info with retries and exponential back-off.

        Returns:
            mt5.account_info() object or None if not retrieved.
        """
        for attempt in range(1, max_attempts + 1):
            info = mt5.account_info()
            if info:
                return info

            self.logger.warning(
                f"[{self.name}] Unable to fetch account info for {self.name}: {self.symbol} "
                f"(attempt {attempt}/{max_attempts})"
            )
            # exponential back-off
            time.sleep(base_delay * (2 ** (attempt - 1)))

        self.logger.error(
            f"[{self.name}] Failed to obtain symbol info for {self.symbol} "
            f"after {max_attempts} attempts."
        )
        return None
            

    def _set_account_info(self, info):
        """ Set account info attributes based on retrieved data."""
        if info is None:
            self.logger.warning("Account info is None, going to default settings.")
            self.leverage = 100
        else:
            if info.leverage == 0:
                self.leverage = 100
            else:
                self.leverage = info.leverage


    def _get_filling_type(self) -> None:
        """
        Set order filling type from broker name (defaults to IOC).
        """
        self.filling_type = FILLING_TYPE_MAP.get(
            self.name, mt5.ORDER_FILLING_IOC
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
