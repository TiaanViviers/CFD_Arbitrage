import requests
import threading
from datetime import datetime, UTC

class TeleBot:
    """
    Minimal Telegram Bot for logging and alerts from CFD Arbitrage system.
    Thread-safe for multi-process usage. No Markdown formatting.
    """

    def __init__(self):
        self.symbol = ''
        self.token = "8057200194:AAHYTBORzzAcMEE2KQ2CmDmFyJSE1VtYjFw"
        self.chat_id = -4940676347
        self._lock = threading.Lock()


    @staticmethod
    def _now_str():
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


    def daily_report(self, num_closed_arbs, num_closed_lims, balances):
        """
        Send a daily summary (can call from a cron job or scheduled event).
        """
        lines = [
            f"📈 Daily Report for {self.symbol}",
            f"----------------",
            f"Time: {self._now_str()},",
            f"Num of closed arb events: {num_closed_arbs}",
            f"Num of closed lim trades: {num_closed_lims}",
            f"Balances:"
        ]
        for broker, bal in balances.items():
            lines.append(f"- {broker}: ${bal:,.2f}")
        self.send_message("\n".join(lines))


    def open_success(self, sell_tr, buy_tr):
        lines = [f"🟢 Trade pair opened successfully on {self.symbol}: {sell_tr.broker}<->{buy_tr.broker}",
                 f"----------------",
                 f"Time: {self._now_str()}",
                 f"Divergence: {(sell_tr.entry_price - buy_tr.entry_price):.2f}",
                 f"Lot Size: {sell_tr.lot_size}"
        ]
        self.send_message("\n".join(lines))


    def open_fail(self, sell_tr, buy_tr):
        lines = [f"🔴 Trade pair failed on {self.symbol}: {sell_tr.broker}<->{buy_tr.broker}",
                 f"----------------",
                 f"Time: {self._now_str()}",
                 f"sell: {sell_tr.status} ({sell_tr.error})",
                 f"buy: {buy_tr.status} ({buy_tr.error})"
        ]
        self.send_message("\n".join(lines))

    
    def close_trade(self, sell_tr, buy_tr):
        lines = [f"🟣 Closed trade pair on {self.symbol}: {sell_tr.broker}<->{buy_tr.broker}",
                 f"----------------",
                 f"Time: {self._now_str()}",
                 f"PnL: {(sell_tr.pnl + buy_tr.pnl):.2f}"
        ]
        self.send_message("\n".join(lines))


    def open_lim(self, lim_tr, win_rate):
        lines = [f"🟢 Opened LIM trade on {self.symbol}: {lim_tr.broker}",
                 f"----------------",
                 f"Time: {self._now_str()}",
                 f"Win Rate: {win_rate:.2f}%"
        ]
        self.send_message("\n".join(lines))

    
    def close_lim(self, lim_tr):
        lines = [f"🟣 Closed LIM trade on {self.symbol}: {lim_tr.broker}",
                 f"----------------",
                 f"Time: {self._now_str()}",
                 f"PnL +/-: {lim_tr.pnl:.2f}"
        ]
        self.send_message("\n".join(lines))


    def set_asset(self, asset):
        self.symbol = asset

