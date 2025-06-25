import MetaTrader5 as mt5
import threading
from datetime import datetime, UTC
import time
import logging

class MT5BrokerInterface:
    def __init__(self, name, path, logger=None):
        self.name = name
        self.path = path
        self.logger = logger or logging.getLogger(__name__)
        self._lock = threading.Lock()
        self.connect()


    def connect(self, max_attempts=10):
        attempts = 0
        while attempts < max_attempts:
            with self._lock:
                if mt5.initialize(self.path):
                    self.logger.info(f"[{self.name}] Successfully connected to MT5 terminal.")
                    return True
                else:
                    e = mt5.last_error()
                    self.logger.warning(f"[{self.name}] MT5 initialization failed, Attempt {attempts+1}: {e}")
            attempts += 1
            time.sleep(2 * (2 ** min(attempts - 1, 3)))

        self.logger.error(f"[{self.name}] Failed to connect to MT5 after {attempts} attempts. Giving up.")
        return False


    def shutdown(self):
        with self._lock:
            try:
                mt5.shutdown()
                self.logger.info(f"[{self.name}] Disconnected from MT5 terminal.")
            except Exception as e:
                self.logger.error(f"[{self.name}] Error during MT5 shutdown: {e}")


    def get_latest_tick(self, symbol):
        with self._lock:
            try:
                tick = mt5.symbol_info_tick(symbol)
                if tick is None:
                    self.logger.warning(f"[{self.name}] No tick data available for {symbol}.")
                    return None
                return {
                    "timestamp": datetime.now(UTC).isoformat(),
                    "bid": tick.bid,
                    "ask": tick.ask,
                }
            except Exception as e:
                self.logger.error(f"[{self.name}] Exception when fetching tick for {symbol}: {e}")
                return None
        

    def is_trade_possible(self, symbol):
        with self._lock:
            try:
                info = mt5.symbol_info(symbol)
                if info is None:
                    self.logger.warning(f"[{self.name}] Failed to get symbol info for {symbol}")
                    return False
                if not info.trade_allowed:
                    self.logger.info(f"[{self.name}] Trading is currently NOT allowed on {symbol}.")
                    return False
                return True
            except Exception as e:
                self.logger.error(f"[{self.name}] Exception checking trade possible for {symbol}: {e}")
                return False
        

    def get_balance(self, retries=2, retry_delay=0.05):
        with self._lock:
            for attempt in range(retries + 1):
                try:
                    account_info = mt5.account_info()
                    if account_info is not None:
                        return account_info.balance
                    else:
                        if attempt < retries:
                            time.sleep(retry_delay)
                except Exception as e:
                    self.logger.error(f"[{self.name}] Exception when getting account info: {e}")
                    if attempt < retries:
                        time.sleep(retry_delay)

            self.logger.error(f"[{self.name}] Could not get account balance after {retries+1} attempts.")
            return None
        
    
    def place_order(self, symbol, side, lots, price=None, sl=None, tp=None, 
                    deviation=20, magic=1000, comment=''):
        with self._lock:
            type_map = {'buy': mt5.ORDER_TYPE_BUY, 'sell': mt5.ORDER_TYPE_SELL}
            if side not in type_map:
                self.logger.error(f"[{self.name}] Invalid order side: {side}. Must be 'buy' or 'sell'.")
                return None

            try:
                tick = mt5.symbol_info_tick(symbol)
                if tick is None:
                    self.logger.error(f"[{self.name}] Failed to fetch tick data for {symbol}.")
                    return None

                exec_price = price
                if exec_price is None:
                    if side == 'buy' and tick.ask > 0:
                        exec_price = tick.ask
                    elif side == 'sell' and tick.bid > 0:
                        exec_price = tick.bid
                    else:
                        self.logger.error(
                            f"[{self.name}] Invalid tick data for {symbol}: bid={tick.bid}, ask={tick.ask}"
                        )
                        return None

                request = {
                    "action": mt5.TRADE_ACTION_DEAL,
                    "symbol": symbol,
                    "volume": lots,
                    "type": type_map[side],
                    "price": exec_price,
                    "sl": sl,
                    "tp": tp,
                    "deviation": deviation,
                    "magic": magic,
                    "comment": comment,
                    "type_time": mt5.ORDER_TIME_GTC,
                    "type_filling": mt5.ORDER_FILLING_IOC,
                }

                self.logger.info(f"[{self.name}] Sending order: {side} {lots} {symbol} @ {exec_price}")
                result = mt5.order_send(request)

                if result is None:
                    self.logger.error(f"[{self.name}] order_send() returned None for {symbol}.")
                    return None

                if result.retcode != mt5.TRADE_RETCODE_DONE:
                    self.logger.error(
                        f"[{self.name}] Order failed for {symbol}: {result.comment} (retcode={result.retcode})"
                    )
                    return result

                self.logger.info(
                    f"[{self.name}] Order placed: {side} {lots} {symbol} @ {exec_price} (ticket: {result.order})"
                )
                return result

            except Exception as e:
                self.logger.error(
                    f"[{self.name}] Exception during order placement for {symbol}: {e}", exc_info=True
                )
                return None
            

    def close_position(self, symbol, ticket=None, volume=None, deviation=20, magic=1000, comment=''):
        """
        Closes open position(s) for the given symbol.
        If `ticket` is provided, only that specific position is closed.
        If `volume` is provided, closes only that lot size; otherwise closes the full position.
        """
        with self._lock:
            try:
                positions = mt5.positions_get(symbol=symbol)
                if positions is None or len(positions) == 0:
                    self.logger.info(f"[{self.name}] No open positions to close for {symbol}.")
                    return True

                for pos in positions:
                    if ticket is not None and pos.ticket != ticket:
                        continue  # Skip if not the ticket we're looking for

                    close_volume = volume if volume is not None else pos.volume

                    # Decide direction and price for closing
                    if pos.type == mt5.POSITION_TYPE_BUY:
                        close_type = mt5.ORDER_TYPE_SELL
                        tick = mt5.symbol_info_tick(symbol)
                        if tick is None or tick.bid <= 0:
                            self.logger.error(f"[{self.name}] No valid bid price to close BUY position {pos.ticket} on {symbol}.")
                            continue
                        price = tick.bid
                    elif pos.type == mt5.POSITION_TYPE_SELL:
                        close_type = mt5.ORDER_TYPE_BUY
                        tick = mt5.symbol_info_tick(symbol)
                        if tick is None or tick.ask <= 0:
                            self.logger.error(f"[{self.name}] No valid ask price to close SELL position {pos.ticket} on {symbol}.")
                            continue
                        price = tick.ask
                    else:
                        self.logger.error(f"[{self.name}] Unknown position type for ticket {pos.ticket}.")
                        continue

                    request = {
                        "action": mt5.TRADE_ACTION_DEAL,
                        "symbol": symbol,
                        "volume": close_volume,
                        "type": close_type,
                        "position": pos.ticket,
                        "price": price,
                        "deviation": deviation,
                        "magic": magic,
                        "comment": comment or "Auto-close",
                        "type_time": mt5.ORDER_TIME_GTC,
                        "type_filling": mt5.ORDER_FILLING_IOC,
                    }

                    self.logger.info(f"[{self.name}] Closing position ticket {pos.ticket}: {close_type} {close_volume} {symbol} @ {price}")
                    result = mt5.order_send(request)
                    if result is None:
                        self.logger.error(f"[{self.name}] order_send() returned None while closing position {pos.ticket} on {symbol}.")
                        return False
                    if result.retcode != mt5.TRADE_RETCODE_DONE:
                        self.logger.error(f"[{self.name}] Failed to close position {pos.ticket}: {result.comment} (retcode={result.retcode})")
                        return False
                    self.logger.info(f"[{self.name}] Closed position ticket {pos.ticket}: {close_type} {close_volume} {symbol} (order: {result.order})")
                return True

            except Exception as e:
                self.logger.error(f"[{self.name}] Exception while closing position(s) for {symbol}: {e}", exc_info=True)
                return False


