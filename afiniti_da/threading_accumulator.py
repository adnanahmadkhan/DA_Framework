from threading import Lock
from abc import ABC, abstractmethod


class ThreadingAccumulator(ABC):
    
    # size = number of things to accumulate before processing
    # serial = ensure only one list process at a time
    #
    def __init__(self, size=512, serial=True):
        self._size = size
        self._serial = serial
        self._list_lock = Lock()
        self._process_lock = Lock()
        self._list = []

    def add(self, thing):
        process_list = None

        with self._list_lock:
            self._list.append(thing)

            if len(self._list) >= self._size:
                process_list = self._list
                self._list = []
        
        if process_list:
            if self._serial:
                with self._process_lock:
                    self._on_process(process_list)
            else:
                self._on_process(process_list)
    
    @abstractmethod
    def _on_process(self, the_list):
        pass

    def flush(self):
        process_list = None

        with self._list_lock:
            if len(self._list) > 0:
                process_list = self._list
                self._list = []
        
        if process_list:
            if self._serial:
                with self._process_lock:
                    self._on_process(process_list)
            else:
                self._on_process(process_list)

