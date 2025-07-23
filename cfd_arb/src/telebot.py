import threading
from datetime import datetime, UTC
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
    def daily_report(
        self, closed_arbs: list, closed_lims: list, balances: dict[str, float]
    ) -> None:
        """Send a daily summary."""
        lines = [
            f"📈 Daily Report for {self.symbol}",
            "----------------",
            f"Total ARB Trades: {len(closed_arbs)}",
            f"#ARB Trades Today: {self._get_today_arbs(closed_arbs)}\n",
            f"Total LIM Trades: {len(closed_lims)}",
            f"#LIM Trades Today: {self._get_today_lims(closed_lims)}\n",
            f"Total PnL: ${self._get_total_pnl(closed_arbs, closed_lims):.2f}",
            f"Today's PnL: ${self._get_today_pnl(closed_arbs, closed_lims):.2f}"
            "----------------",
        ]
        for broker, bal in balances.items():
            lines.append(
                f"{broker}:",
                f"Balance ${bal:.2f}",
                f"PnL: ${self._get_broker_pnl(broker, closed_arbs, closed_lims)}",
                f"Total ARB Trades: {self._get_broker_arbs(broker, closed_arbs)}",
                f"Total LIM Trades: {self._get_broker_lims(broker, closed_lims)}",
                f"----------------",
            )
        self._send_message("\n".join(lines))

    def open_success(self, sell_tr: Trade, buy_tr: Trade) -> None:
        """Notify trade pair opened successfully."""
        lines = [
            f"🟢 Trade pair opened successfully on {self.symbol}: "
            f"{sell_tr.broker}<->{buy_tr.broker}",
            "----------------",
            f"Time: {self._now_str()}",
            f"Divergence: {(sell_tr.entry_price - buy_tr.entry_price):.2f}",
            f"Lot Size: {sell_tr.lot_size}",
        ]
        self._send_message("\n".join(lines))

    def open_fail(self, sell_tr: Trade, buy_tr: Trade) -> None:
        """Notify trade pair open failed."""
        lines = [
            f"🔴 Trade pair failed on {self.symbol}: "
            f"{sell_tr.broker}<->{buy_tr.broker}",
            "----------------",
            f"Time: {self._now_str()}",
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
            f"Time: {self._now_str()}",
            f"PnL: {(sell_tr.pnl + buy_tr.pnl):.2f}",
        ]
        self._send_message("\n".join(lines))

    def open_lim(self, lim_tr: Trade, win_rate: float) -> None:
        """Notify LIM trade opened."""
        lines = [
            f"🟢 Opened LIM trade on {self.symbol}: {lim_tr.broker}",
            "----------------",
            f"Time: {self._now_str()}",
            f"Win Rate: {win_rate:.2f}%",
        ]
        self._send_message("\n".join(lines))

    def close_lim(self, lim_tr: Trade) -> None:
        """Notify LIM trade closed."""
        lines = [
            f"🟣 Closed LIM trade on {self.symbol}: {lim_tr.broker}",
            "----------------",
            f"Time: {self._now_str()}",
            f"PnL +/-: {lim_tr.pnl:.2f}",
        ]
        self._send_message("\n".join(lines))


    ############################# Update Helpers ###############################



