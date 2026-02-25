import time

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
        if not user_id or not semantic_type:
            return False
        if self.cooldown_sec <= 0:
            return False

        key = (user_id, semantic_type)
        now = time.time()

        last_time = self.last_emitted.get(key)
        if last_time is None:
            return False

        return (now - last_time) < self.cooldown_sec

    def update_emit_time(self, user_id: str, semantic_type: str):
        if not user_id or not semantic_type:
            return
        self.last_emitted[(user_id, semantic_type)] = time.time()