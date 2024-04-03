
from joblib import Parallel, delayed
from datetime import datetime
from threading import Lock
import time

import random
start = datetime.now()

class Tracker:
    def __init__(self, max_rate):
        self.max_rate = max_rate
        self._tokens_per_minute = {}
        self._lock  = Lock()
        self._start = datetime.now()


    def minutes_since_start(self):
        return int((datetime.now() - start).total_seconds())

    def add(self, tokens):
        minutes = self.minutes_since_start()
        with self._lock:
            self._tokens_per_minute[minutes] = self._tokens_per_minute.get(minutes, 0) + tokens

    def rate(self):
        minutes = self.minutes_since_start()
        with self._lock:
            tokens = self._tokens_per_minute.get(minutes, 0)
            if minutes != 0:
                tokens+=self._tokens_per_minute.get(minutes-1, 0)
            return tokens

    def wait_until_ready(self):
        while self.rate() > self.max_rate:
            print("Too fast!")
            time.sleep(1)

used_tokens = []

def work(item):
    time.sleep(random.randint(200, 500) / 1000.0)
    return 1, "my result"

def rate_limited_worker(item, tracker):
    tracker.wait_until_ready()

    tokens, result = work(item)
    print(f"Got a result, spent {tokens} tokens")

    tracker.add(tokens)

    return result

if __name__ == "__main__":
    t = Tracker(3)
    res = Parallel(n_jobs=1, require='sharedmem')(delayed(rate_limited_worker)(i, t) for i in range(20))
