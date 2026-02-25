import time

class StateStore:
    def __init__(self, config):
        self.seen_windows = set()
        self.last_emitted = {}
        self.history = {}
        self.cooldown_sec = config["cooldown_sec"]

    def is_duplicate(self, event):
        key = (
            event["run_id"],
            event["user_id"],
            event["window_end_ms"]
        )
        if key in self.seen_windows:
            return True

        self.seen_windows.add(key)
        return False

    def in_cooldown(self, user_id, semantic_type):
        key = (user_id, semantic_type)
        now = time.time()

        if key not in self.last_emitted:
            return False

        last_time = self.last_emitted[key]
        return (now - last_time) < self.cooldown_sec

    def update_emit_time(self, user_id, semantic_type):
        self.last_emitted[(user_id, semantic_type)] = time.time()