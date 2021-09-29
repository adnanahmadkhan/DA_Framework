import logging
import time
import threading


class ThreadingSuccessTracker():

    def __init__(self, success_rate=0.95, check_seconds=30):
        self.track_lock = threading.Lock()  # To get lock for track success rate
        self.track_success = 0
        self.track_total = 0
        self.track_begin = 0

        self.success_rate = success_rate
        self.check_seconds = check_seconds

    def track(self, success):

        with self.track_lock:

            # Increment for this tracking event
            if success:
                self.track_success += 1

            self.track_total += 1

            # Now check if it has been 30 seconds...
            # If so, perform check.
            now = time.monotonic()

            # First call, setup track_begin and we're done...
            if self.track_begin == 0:
                self.track_begin = now
                return True

            # Has not been long enough?
            if now - self.track_begin < self.check_seconds:
                return True

            # If we're here, it's been 30 seconds, perform check...
            # Start by resetting track_begin
            self.track_begin = now

            # We HAVE tracked something, let's calculate success rate
            logging.info("success %s / total %s" % (str(self.track_success), str(self.track_total)))
            this_success_rate = self.track_success / self.track_total
            self.track_success = 0
            self.track_total = 0

            if this_success_rate < self.success_rate:
                logging.error("Success rate too low, rate: " + str(this_success_rate))
                #print(this_success_rate)
                return False

            logging.info("Success rate good, rate: " + str(this_success_rate))
            return True
