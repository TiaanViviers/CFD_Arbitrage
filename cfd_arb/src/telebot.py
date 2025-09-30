import threading
from datetime import datetime, UTC, timedelta
import requests

from trade import Trade


class TeleBot:
    """
    Minimal Telegram Bot for logging and alerts from CFD Arbitrage system.
    Thread-safe for multi-process usage. No Markdown formatting.
    """

    ############################### Init & State ###############################
    def __init__(self) -> None:
        self.symbol: str = ''
        self.starting_capital = None
        self.token: str = (
            "8057200194:AAHYTBORzzAcMEE2KQ2CmDmFyJSE1VtYjFw"
        )
        self.chat_id: int = -4940676347
        self._lock = threading.Lock()


    ################################ Utilities #################################
    @staticmethod
    def _now_str() -> str:
        """Current UTC datetime as YYYY-MM-DD HH:MM string."""
        return datetime.now(UTC).strftime("%Y-%m-%d %H:%M")
    
    def set_asset(self, asset: str) -> None:
        """Set the current trading symbol for all alerts."""
        self.symbol = asset

    def set_starting_capital(self, balances_df) -> None:
        """Set the starting capital from initial balances."""
        starting_cap = round(self._sum_balances(balances_df), 2)
        if starting_cap:
            self.starting_capital = starting_cap
            print(f"[TeleBot] Starting capital set to ${self.starting_capital}")


    ################################ Core Send #################################
    def _send_message(self, message: str) -> None:
        """Send plain text message to Telegram group."""
        with self._lock:
            url = (
                f"https://api.telegram.org/bot{self.token}/sendMessage"
            )
            payload = {"chat_id": self.chat_id, "text": message}
            try:
                resp = requests.post(url, data=payload, timeout=3)
                if not resp.ok:
                    print(f"[TeleBot] Failed to send: {resp.text}")
            except Exception as e:
                print(f"[TeleBot] Exception: {e}")


    ############################# Message Builders #############################
    def daily_report(self, closed_arbs: list, balances: dict[str, float]) -> None:
        """Send a daily summary."""
        port_value = round(self._sum_balances(balances), 2)
        pnl = port_value - self.starting_capital
        lines = [
            f"📈 Daily Report for {self.symbol}",
            "----------------",
            f"Total Closed Trades: {len(closed_arbs)}",
            f"#Closed Trades Today: {self._get_today_arbs(closed_arbs)}\n",
            f"Starting Portfolio Value: ${self.starting_capital:.2f}",
            f"Total Portfolio Value: ${port_value:.2f}",
            f"Total Portfolio PnL: ${pnl} ({pnl / self.starting_capital * 100:.2f}%)",
            "----------------",
        ]
        for broker, bal in balances.items():
            lines.extend([
                f"{broker}:",
                f"Balance ${bal:.2f}",
                f"PnL: ${self._get_broker_pnl(broker, closed_arbs)}",
                f"Total ARB Trades: {self._get_broker_arbs(broker, closed_arbs)}",
                f"----------------",
            ])
        self._send_message("\n".join(lines))

    def open_success(self, sell_tr: Trade, buy_tr: Trade) -> None:
        """Notify trade pair opened successfully."""
        lines = [
            f"🟢 Trade pair opened successfully on {self.symbol}: "
            f"{sell_tr.broker}(s)<->{buy_tr.broker}(b)",
            "----------------",
            f"Divergence: {(sell_tr.entry_price - buy_tr.entry_price):.2f}",
            f"Lot Size: {sell_tr.lot_size}(s), {buy_tr.lot_size}(b)",
        ]
        self._send_message("\n".join(lines))

    def open_fail(self, sell_tr: Trade, buy_tr: Trade) -> None:
        """Notify trade pair open failed."""
        lines = [
            f"🔴 Trade pair failed on {self.symbol}: "
            f"{sell_tr.broker}<->{buy_tr.broker}",
            "----------------",
            f"sell: {sell_tr.status} ({sell_tr.error})",
            f"buy: {buy_tr.status} ({buy_tr.error})",
        ]
        self._send_message("\n".join(lines))

    def close_trade(self, sell_tr: Trade, buy_tr: Trade) -> None:
        """Notify trade pair closed."""
        lines = [
            f"🟣 Closed trade pair on {self.symbol}: "
            f"{sell_tr.broker}<->{buy_tr.broker}",
            "----------------",
            f"PnL: {(sell_tr.pnl + buy_tr.pnl):.2f}",
        ]
        self._send_message("\n".join(lines))


    ############################# Update Helpers ###############################
    def _sum_balances(self, balances) -> float:
        """Sum balances dict/Series/iterable, ignoring None and NaN."""
        total = 0.0
        for _, bal in balances.items():
            total += bal if bal is not None else 0.0

        return total
    

    def _get_today_arbs(self, closed_arbs):
        """
        Count arbitrage trades closed after 21:00 UTC of the previous day.
        Args:
            closed_arbs: List of closed arbitrage trade pairs
        Returns:
            int: Number of trades closed after the cutoff time
        """
        now = datetime.now(UTC)
        # Set cutoff time to 21:00 UTC of the previous day
        cutoff = now.replace(hour=21, minute=0, second=0, microsecond=0)
        if now.hour < 21:
            cutoff = cutoff - timedelta(days=1)
        
        arbs = 0
        for pair in closed_arbs:
            # Parse the ISO format close time string
            close_time = datetime.fromisoformat(pair[0].close_time)
            if close_time >= cutoff:
                arbs += 1
        return arbs
    

    def _get_broker_pnl(self, broker, closed_arbs):
        """
        Calculate total PnL for a specific broker.
        Args:
            broker: Broker name to filter trades
            closed_arbs: List of closed arbitrage trade pairs
        Returns:
            float: Total PnL for the broker
        """
        total_pnl = 0.0
        for pair in closed_arbs:
            if pair[0].broker == broker and pair[0].pnl is not None:
                total_pnl += pair[0].pnl
            elif pair[1].broker == broker and pair[1].pnl is not None:
                total_pnl += pair[1].pnl
        
        return total_pnl


    def _get_broker_arbs(self, broker, closed_arbs):
        """
        Count total arbitrage trades for a specific broker.
        Args:
            broker: Broker name to filter trades
            closed_arbs: List of closed arbitrage trade pairs
        Returns:
            int: Number of trades for the broker
        """
        count = 0
        for pair in closed_arbs:
            if pair[0].broker == broker or pair[1].broker == broker:
                count += 1
        return count
