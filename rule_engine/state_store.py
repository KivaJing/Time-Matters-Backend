import time
from collections import deque
from typing import Dict, Tuple, Optional, List

MAX_SEEN = 100000


class StateStore:
    def __init__(self, config: dict):
        self.seen_windows = set()
        self.history = {}

        # Track last dominant semantic state per (run_id, user_id)
        self.last_selected_state: Dict[Tuple[str, str], Tuple[str, str]] = {}
        self.last_selected_time: Dict[Tuple[str, str], float] = {}

        self.history_window_size = (
            config.get("history_window_size")
            if isinstance(config, dict) and "history_window_size" in config
            else config.get("rule_engine", {}).get("history_window_size", 5)
        )
        if self.history_window_size is None or self.history_window_size <= 0:
            self.history_window_size = 5

        self.emit_same_state_after_sec = (
            config.get("rule_engine", {}).get("emit_same_state_after_sec", 30.0)
            if isinstance(config, dict)
            else 30.0
        )
        if self.emit_same_state_after_sec is None or self.emit_same_state_after_sec < 0:
            self.emit_same_state_after_sec = 30.0

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

    def add_history(self, user_id: str, event: dict):
        if not user_id:
            return
        if user_id not in self.history:
            self.history[user_id] = deque(maxlen=self.history_window_size)
        self.history[user_id].append(event)

    def get_recent_values(self, user_id: str, field: str, windows: int = 3) -> List[float]:
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

    def get_selected_state(self, run_id: str, user_id: str) -> Optional[Tuple[str, str]]:
        if not run_id or not user_id:
            return None
        return self.last_selected_state.get((run_id, user_id))

    def get_selected_state_age_sec(self, run_id: str, user_id: str) -> Optional[float]:
        if not run_id or not user_id:
            return None
        last_time = self.last_selected_time.get((run_id, user_id))
        if last_time is None:
            return None
        return time.time() - last_time

    def update_selected_state(self, run_id: str, user_id: str, semantic_type: str, severity: str):
        if not run_id or not user_id or not semantic_type or not severity:
            return
        key = (run_id, user_id)
        self.last_selected_state[key] = (semantic_type, severity)
        self.last_selected_time[key] = time.time()

    def should_emit_on_state_change(
        self,
        run_id: str,
        user_id: str,
        semantic_type: str,
        severity: str,
        max_same_state_silence_sec: Optional[float] = None,
    ) -> bool:
        """
        Emit when:
        1. no previous dominant state exists
        2. semantic_type changed
        3. severity changed
        4. same state persists beyond TTL and should be re-emitted
        """
        if not run_id or not user_id or not semantic_type or not severity:
            return False

        ttl = self.emit_same_state_after_sec if max_same_state_silence_sec is None else max_same_state_silence_sec
        if ttl is None or ttl < 0:
            ttl = 30.0

        key = (run_id, user_id)
        current = (semantic_type, severity)
        previous = self.last_selected_state.get(key)

        if previous is None:
            return True

        if current != previous:
            return True

        last_time = self.last_selected_time.get(key)
        if last_time is None:
            return True

        return (time.time() - last_time) >= ttl