import time
from collections import deque

MAX_SEEN = 100000


class StateStore:
    def __init__(self, config: dict):
        self.seen_windows = set()
        self.last_emitted = {}
        self.history = {}

        self.cooldown_sec = (
            config.get("cooldown_sec")
            if isinstance(config, dict) and "cooldown_sec" in config
            else config.get("rule_engine", {}).get("cooldown_sec", 0)
        )
        if self.cooldown_sec is None:
            self.cooldown_sec = 0

        self.history_window_size = (
            config.get("history_window_size")
            if isinstance(config, dict) and "history_window_size" in config
            else config.get("rule_engine", {}).get("history_window_size", 5)
        )
        if self.history_window_size is None or self.history_window_size <= 0:
            self.history_window_size = 5

    def is_duplicate(self, event: dict) -> bool:
        run_id = event.get("run_id")
        user_id = event.get("user_id")
        window_end = event.get("window_end_ms")
        window_start = event.get("window_start_ms")

        if not run_id or not user_id or window_end is None:
            return False

        key = (run_id, user_id, window_start, window_end)
        if key in self.seen_windows:
            return True

        self.seen_windows.add(key)
        if len(self.seen_windows) > MAX_SEEN:
            self.seen_windows.clear()
        return False

    def in_cooldown(self, user_id: str, semantic_type: str) -> bool:
        if not user_id or not semantic_type or self.cooldown_sec <= 0:
            return False
        key = (user_id, semantic_type)
        last_time = self.last_emitted.get(key)
        if last_time is None:
            return False
        return (time.time() - last_time) < self.cooldown_sec

    def update_emit_time(self, user_id: str, semantic_type: str):
        if user_id and semantic_type:
            self.last_emitted[(user_id, semantic_type)] = time.time()

    def add_history(self, user_id: str, event: dict):
        if not user_id:
            return
        if user_id not in self.history:
            self.history[user_id] = deque(maxlen=self.history_window_size)
        self.history[user_id].append(event)

    def get_recent_values(self, user_id: str, field: str, windows: int = 3):
        items = list(self.history.get(user_id, []))[-windows:]
        values = []
        for item in items:
            value = item.get(field)
            if value is not None:
                try:
                    values.append(float(value))
                except (ValueError, TypeError):
                    continue
        return values

    def is_rising(self, user_id: str, field: str, windows: int = 3, delta: float = 2.0) -> bool:
        vals = self.get_recent_values(user_id, field, windows)
        if len(vals) < windows:
            return False
        monotonic = all(vals[i] < vals[i + 1] for i in range(len(vals) - 1))
        return monotonic and (vals[-1] - vals[0] >= delta)

    def is_sustained_high(self, user_id: str, field: str, windows: int = 3, threshold: float = 7.0) -> bool:
        vals = self.get_recent_values(user_id, field, windows)
        return len(vals) == windows and all(v >= threshold for v in vals)
