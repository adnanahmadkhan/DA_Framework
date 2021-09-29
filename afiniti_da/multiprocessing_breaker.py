from multiprocessing import Queue
from multiprocessing import Event
import queue
import time

# A "software breaker" designed to operate across multiple processes via the
# multiprocessing.Event class.
#
class MultiprocessingBreaker:
    
    # Param granularity controls how often the underlying multiprocessing Event
    # is checked vs using a cached value.  This is designed to remove the 10x 
    # overhead of the IPC for multiprocessing events in situations where a less
    # graunlar response to breaker tripping is acceptable.  The default is 0.1
    # seconds (100ms).  Set to lower values for higher granularity.
    #
    def __init__(self, granularity=0.1):
        self._cached = False
        self._checked = time.monotonic()
        self._granularity = granularity
        self._event = Event()
        # Queue stories the reason the breaker was tripped for consumption
        self._reason = Queue(maxsize=1)
    
    # Trip the breaker for some reason.  Reason can be any type that can go in a queue.
    #
    def trip(self, reason):

        try:
            self._reason.put(obj=reason, block=False)
            self._event.set()
            self._cached = True
        except queue.Full:
            pass
    
    # Granularity from constructor can be overridden here for individual calls
    #
    def is_tripped(self, granularity=None):
        use_granularity = self._granularity

        if granularity is not None:
            use_granularity = granularity
            
        if (use_granularity <= 0) or (time.monotonic() - self._checked >= use_granularity):
            self._cached = self._event.is_set()
            self._checked = time.monotonic()

        return self._cached
    
    # This can only be called once if is_tripped is true.  
    # Should only be called by a "master/parent process" during cleanup/shutdown
    # about the reason the breaker was tripped elsewhere.
    # Will throw if called when breaker was not tripped or if called again as queue will be empty.
    #
    def consume_reason(self):
        return self._reason.get(block=False)
