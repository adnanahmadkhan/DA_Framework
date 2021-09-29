from multiprocessing import Queue, Event
from threading import Thread
import queue
import logging

class MultiprocessingFileCollector:

    def __init__(self, filePath, queue_maxsize=512, mode="a", encoding="utf-8"):
        super().__init__()
        self._queue = Queue(maxsize=queue_maxsize)
        self._event = Event()
        self._filePath = filePath
        self._mode = mode
        self._encoding = encoding
    
    def collect(self, item):
        self._queue.put(item)

    # Entry point for work thread.
    #
    def _work_thread_entry(self):
        
        f = open(self._filePath, mode=self._mode, encoding=self._encoding)

        while True:
            item = None
            try:
                item = self._queue.get(timeout=1) # throws on timeout
                f.write(item)
            except queue.Empty:
                if self._event.is_set():
                    logging.debug("_work_thread_entry(): Done.")
                    break

        f.close()
        
    # Starts the work thread that handles all collections.  Returns thread so
    # that it can be joined later.
    #
    def start(self):
        thread = Thread(target=self._work_thread_entry)
        thread.start()
        return thread
    
    # Sends the event that causes the work thread to shut down.
    #
    def stop(self):
        self._event.set()

