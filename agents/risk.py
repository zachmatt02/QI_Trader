# agents/risk.py
"""Hard trading limits for the strategy agent.

The decision maker (rule-based or AI) only proposes trades; RiskManager can
veto every one of them. All hard limits live here, in code — never delegate
them to a model.

Rails (env-overridable):
  * MAX_POSITION=100            largest absolute position, in shares
  * MAX_ORDER_NOTIONAL=25000    largest single order, in account currency
  * ORDER_COOLDOWN_SECONDS=300  minimum seconds between two order attempts
  * MAX_DAILY_LOSS=1000         realized loss per day that halts trading
  * Kill switch: `touch data/KILL_SWITCH` blocks all orders until removed.

Position, realized P&L and cooldown survive restarts in data/risk/<ticker>.json.
"""
import json
import os
import time
from datetime import date
from pathlib import Path

DATA_ROOT = Path(__file__).resolve().parent.parent / "data"
KILL_SWITCH = DATA_ROOT / "KILL_SWITCH"

MAX_POSITION = int(os.environ.get("MAX_POSITION", "100"))
MAX_ORDER_NOTIONAL = float(os.environ.get("MAX_ORDER_NOTIONAL", "25000"))
ORDER_COOLDOWN_SECONDS = float(os.environ.get("ORDER_COOLDOWN_SECONDS", "300"))
MAX_DAILY_LOSS = float(os.environ.get("MAX_DAILY_LOSS", "1000"))


class RiskManager:
    def __init__(self, ticker):
        self.ticker = ticker
        safe = "".join(c if c.isalnum() or c in ".-" else "_" for c in ticker)
        self._path = DATA_ROOT / "risk" / f"{safe}.json"
        self.position = 0            # signed shares (negative = short)
        self.avg_cost = 0.0
        self.realized_pnl_today = 0.0
        self.pnl_date = date.today().isoformat()
        self.last_order_ts = 0.0
        if self._path.exists():
            state = json.loads(self._path.read_text())
            self.position = int(state.get("position", 0))
            self.avg_cost = float(state.get("avg_cost", 0.0))
            self.realized_pnl_today = float(state.get("realized_pnl_today", 0.0))
            self.pnl_date = state.get("pnl_date", self.pnl_date)
            self.last_order_ts = float(state.get("last_order_ts", 0.0))
        self._roll_day()

    def check(self, side, quantity, price):
        """Returns the veto reason as a string, or None when the order may go
        ahead. Never raises."""
        self._roll_day()
        if KILL_SWITCH.exists():
            return f"kill switch is set — remove {KILL_SWITCH} to resume"
        if self.realized_pnl_today <= -MAX_DAILY_LOSS:
            return (f"daily loss limit: realized {self.realized_pnl_today:+.2f} "
                    f"today (limit -{MAX_DAILY_LOSS:.2f})")
        wait = ORDER_COOLDOWN_SECONDS - (time.time() - self.last_order_ts)
        if wait > 0:
            return f"cooldown: next order allowed in {wait:.0f}s"
        notional = quantity * price
        if notional > MAX_ORDER_NOTIONAL:
            return (f"order notional {notional:.2f} exceeds "
                    f"MAX_ORDER_NOTIONAL {MAX_ORDER_NOTIONAL:.2f}")
        new_position = self.position + (quantity if side == "BUY" else -quantity)
        if abs(new_position) > MAX_POSITION:
            return (f"position would become {new_position:+d} shares "
                    f"(limit ±{MAX_POSITION})")
        return None

    def record_order(self):
        """Starts the cooldown. Call on every order attempt, previews too, so
        a broken loop cannot hammer the gateway."""
        self.last_order_ts = time.time()
        self._save()

    def record_fill(self, side, quantity, price):
        """Updates position and realized P&L (average-cost accounting).
        Returns today's realized P&L."""
        signed = quantity if side.upper() == "BUY" else -quantity
        if self.position == 0 or (self.position > 0) == (signed > 0):
            # opening or adding: blend the average cost
            total = abs(self.position) + abs(signed)
            self.avg_cost = (self.avg_cost * abs(self.position)
                             + price * abs(signed)) / total
        else:
            # reducing (or flipping through zero): realize P&L on what closes
            closing = min(abs(signed), abs(self.position))
            direction = 1 if self.position > 0 else -1
            self.realized_pnl_today += (price - self.avg_cost) * closing * direction
            if abs(signed) > abs(self.position):
                self.avg_cost = price  # the remainder opens a new position
        self.position += signed
        if self.position == 0:
            self.avg_cost = 0.0
        self._save()
        return self.realized_pnl_today

    def _roll_day(self):
        today = date.today().isoformat()
        if self.pnl_date != today:
            self.pnl_date = today
            self.realized_pnl_today = 0.0

    def _save(self):
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps({
            "position": self.position,
            "avg_cost": self.avg_cost,
            "realized_pnl_today": self.realized_pnl_today,
            "pnl_date": self.pnl_date,
            "last_order_ts": self.last_order_ts,
        }, indent=2))
