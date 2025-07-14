import requests
import threading
from datetime import datetime, UTC

class TeleBot:
    """
    Minimal Telegram Bot for logging and alerts from CFD Arbitrage system.
    Thread-safe for multi-process usage. No Markdown formatting.
    """

    def __init__(self):
        self.token = "8057200194:AAHYTBORzzAcMEE2KQ2CmDmFyJSE1VtYjFw"
        self.chat_id = -4940676347
        self._lock = threading.Lock()

    @staticmethod
    def _now_str():
        # Returns UTC now as YYYY-MM-DD HH:MM
        return datetime.now(UTC).strftime("%Y-%m-%d %H:%M")

    def send_message(self, message):
        """
        Sends a raw message to the Telegram group, plain text only.
        """
        with self._lock:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            payload = {
                "chat_id": self.chat_id,
                "text": message
            }
            try:
                resp = requests.post(url, data=payload, timeout=3)
                if not resp.ok:
                    print(f"[TeleBot] Failed to send: {resp.text}")
            except Exception as e:
                print(f"[TeleBot] Exception: {e}")

    def alert(self, msg, *, symbol=None, critical=False):
        """Send an urgent alert, e.g., unexpected exposure or broker error."""
        time = self._now_str()
        prefix = "‼️ CRITICAL ALERT" if critical else "⚠️ Alert"
        lines = [f"{prefix} at {time}"]
        if symbol:
            lines.append(f"Symbol: {symbol}")
        lines.append("----")
        lines.append(str(msg))
        self.send_message("\n".join(lines))

    def error(self, error_msg, context=None):
        """Send an error message."""
        time = self._now_str()
        lines = [f"❌ Error at {time}"]
        if context:
            lines.append(f"Context: {context}")
        lines.append("----")
        lines.append(str(error_msg))
        self.send_message("\n".join(lines))

    def trade_event(self, event_type, broker, symbol, details):
        """
        Send trade-related event: open, close, SL/TP, orphan, etc.
        """
        time = self._now_str()
        em = {
            "Opened": "🟢",
            "Closed": "🔵",
            "SL Hit": "🔴",
            "TP Hit": "🟣",
            "Orphan": "⚫️",
        }.get(event_type, "ℹ️")
        lines = [
            f"{em} Trade {event_type} at {time}",
            f"Broker: {broker}",
            f"Symbol: {symbol}",
            "----"
        ]
        for k, v in details.items():
            lines.append(f"{k.title()}: {v}")
        self.send_message("\n".join(lines))

    def daily_report(self, balances, trade_stats):
        """
        Send a daily summary (can call from a cron job or scheduled event).
        """
        time = self._now_str()
        lines = [f"📈 Daily Report — {time}", ""]
        lines.append("Balances:")
        for broker, bal in balances.items():
            lines.append(f"- {broker}: ${bal:,.2f}")
        lines.append("")
        lines.append("Trade Stats:")
        for stat, val in trade_stats.items():
            lines.append(f"- {stat.title()}: {val}")
        self.send_message("\n".join(lines))


# test client 
if __name__ == "__main__":
    bot = TeleBot()
    bot.alert("This is a test alert!", symbol="BTCUSD", critical=True)
    bot.error("API timed out when requesting tick data", context="worker_proc(fxtm)")
    bot.trade_event("Opened", "icmarkets", "BTCUSD", {
        "side": "buy", "lot": 0.1, "entry": 58000
    })
    bot.daily_report(
        balances={"icmarkets": 12232.42, "exness": 13500.51},
        trade_stats={"total": 5, "winrate": "60%"}
    )
