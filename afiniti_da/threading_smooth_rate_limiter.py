import datetime
import time
import logging
from threading import Lock


class ThreadingSmoothRateLimiter:

    def __init__(self, calls_per_second=5, report_interval_seconds=5):
        self._lock = Lock()
        self._last_time = 0
        self._last_report = 0
        self._delay = 1/calls_per_second
        self._report_interval_seconds = report_interval_seconds
        self._report_count = 0

    def wait(self):
        with self._lock:
            entry_time = time.monotonic()

            if self._last_report == 0:
                self._last_report = entry_time
            elif self._last_report < entry_time - self._report_interval_seconds:
                logging.info("ThreadingSmoothRateLimiter rate for THIS process: %f/s" % (self._report_count / (entry_time - self._last_report)))
                self._report_count = 0
                self._last_report = entry_time

            elapsed = entry_time - self._last_time
            remaining = self._delay - elapsed

            if remaining > 0:
                time.sleep(remaining)
                after_sleep = entry_time + remaining
            else:
                after_sleep = entry_time
            
            self._last_time = after_sleep
            self._report_count += 1
