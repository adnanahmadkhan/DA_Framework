from abc import ABC, abstractmethod
from multiprocessing import Process, Queue, Event
from .multiprocessing_breaker import MultiprocessingBreaker
from .breaker_exceptions import BreakerTrippedException, BreakerTrippingException
from .threading_smooth_rate_limiter import ThreadingSmoothRateLimiter
import queue
import math
import logging
import argparse
from .threading_bounded_executor import ThreadingBoundedExecutor

# A framework base class designed to be extended and customized for workloads
# that need to run across multiple threads on multiple processor cores.
# 
# Simply extend this class and implement the following two abstract methods:
#   _acquire_work()
#       Acquire work items and call _distribute_work() on each item to work.
#   _on_work()
#       Receive distributed item and work it.
#
# Additionally you may implement the following methods to setup logging and
# database connections for each separate spawned process:
#   _on_acquisition_process_setup()
#   _on_work_process_setup()
#
# If you accumulate writes or connect to external resources, you may implement
# the following methods to flush any remaining writes and cleanup database 
# or open files as needed:
#   _on_acquisition_process_complete()
#   _on_work_process_complete()

class MultiprocessingThreadingWorkQueue(ABC):
    
    # Note that the thread pools will always keep a queue of one work item per thread ready to
    # go to ensure each thread has an item ready without having to suffer the delay of an IPC read
    # via the main multiprocessing queue
    # 
    # For example, with...
    #   4 processes
    #   1024 max_threads_per_process
    #   4096 queue_maxsize
    #
    # The resulting configuration will have...
    #   1 mainline python process
    #       1 multiprocessing queue of 4096 depth for work items
    #       1 spawned work acquisition thread
    #       4 spawned work processes each with...
    #           1024 work threads each
    #           1024 backlog work queue
    #
    # This means there are potentially...
    #   4096 work items being worked at once (active work threads)
    #   4096 work items in queue in work processes
    #   4096 work items in the multiprocessing queue
    #
    # Total: 4096 parallel work items in progress with a backlog of 8192.
    #
    def __init__(self, work_processes=1, max_threads_per_work_process=1024, queue_maxsize=1024, rate=10):
        super().__init__()
        self._work_processes = work_processes
        self._max_threads_per_work_process = max_threads_per_work_process
        self._queue_maxsize = queue_maxsize
        self._rate = rate
        self._configured = True
    
    # A method that will override constructor configuration values from 
    # a standardized set of program arguments.  While *technically* you
    # can avoid calling __init__ if you call this, you should *always*
    # call the super().__init__() method when you derive from a base class
    # if you override the __init__() method in your derived class.
    #
    # Pass in a parser that has your own arguments added.  Method will
    # return the parsed arguments object.  If no parser is passed in,
    # a default blank one will be made.
    #
    def argparse(self, parser=None):
        if not parser:
            parser = argparse.ArgumentParser()

        parser.add_argument("--processes", default=8, type=int, help="number of work processes")
        parser.add_argument("--threads", default=5, type=int, help="maximum threads PER work process")
        parser.add_argument("--queue", default=1000, type=int, help="depth of multiprocess work queue")
        parser.add_argument("--rate", default=100000, type=int, help="maximum smoothed total work rate per second across all processes and threads")
        args = parser.parse_args()

        self._work_processes = int(args.processes)
        self._max_threads_per_work_process = int(args.threads)
        self._queue_maxsize = int(args.queue)
        self._rate = int(args.rate)
        self._configured = True

        return args
    
    # Call to start the work queue.  Will spawn child processe for acquiring work
    # as well as child processes with thread pools for distributed work items.
    # Blocks until all child processes exit.
    #
    # If the breakers is tripped and causes an abnormal exit, a BreakerException
    # will be raised from which the reason can be recieved.
    #
    def run(self):
        if not hasattr(self, '_configured'):
            raise UnconfiguredException()

        # Each work process gets this rate limit
        self._process_rate = float(self._rate) / float(self._work_processes)
        # The queue that feeds the work threads
        self._queue = Queue(maxsize=self._queue_maxsize)
        # The event that lets everyone know there are no more work items coming
        self._done = Event() 
        # The breaker we use to bail on error
        self._breaker = MultiprocessingBreaker()
        
        # NOTE: From this point on in sub processes, self is a different divergent copy!
        procs = []
        procs.append(Process(target=self._acquisition_process_entry))
        
        for i in range(self._work_processes):
            procs.append(Process(target=self._work_process_entry))

        for p in procs:
            p.start()

        for p in procs:
            p.join()

        if self._breaker.is_tripped():
            raise BreakerTrippedException(reason=self._breaker.consume_reason())


    # Method called by derived class to distribute work acquired
    # in the _acquire_work method.  Note that this method will
    # raise the BreakerTrippingException if the breaker is tripped
    # while it is distributing work.  Be sure to not swallow that
    # exception as the workers will stop and work will cease.
    #
    def _distribute_work(self, item):
        while True:
            try:
                if self._breaker.is_tripped():
                    raise BreakerTrippingException()

                self._queue.put(item, timeout=1)
                break
            except queue.Full:
                pass
            
            
    
    # Internal method for the lifecycle of acquisition process
    #
    def _acquisition_process_entry(self):
        
        try:
            self._on_acquisition_process_setup()
            self._acquire_work()
            self._done.set()
        except BreakerTrippingException:
            logging.error("_acquisition_process_entry(): Acquisition aborted due to tripped breaker.")
        # We catch BaseException here to trip the breaker on keyboard interrupt as well
        # but we always re-throw to properly exit the child process if needed
        except BaseException as e: 
            reason = "_acquisition_process_entry(): Tripping breaker due to exception and re-raising"
            logging.exception(reason)
            self._breaker.trip(reason)
            raise e # re-throw to allow exception to flow through
        finally:
            try:
                self._on_acquisition_process_complete()
            except BaseException as e:
                logging.exception("_acquisition_process_entry(): _on_acquisition_process_complete() raised exception.")
    
    # Internal method for the lifecycle of a work process.
    #
    def _work_process_entry(self):
        executor = None
        
        try:
            self._on_work_process_setup()

            self._rate_limiter = ThreadingSmoothRateLimiter(calls_per_second=self._process_rate)
            executor = ThreadingBoundedExecutor(max_workers=self._max_threads_per_work_process,bound=self._max_threads_per_work_process)

            while True:
                try:
                    if self._breaker.is_tripped():
                        logging.error("_work_process_entry(): Detected tripped breaker.  Exiting.")
                        break
                    
                    item = self._queue.get(timeout=1)          # throws on timeout
                    executor.submit(self._work_thread_entry, item)  # dispatch to threads
                
                except queue.Empty:
                    if self._done.is_set():
                        logging.debug("_work_process_entry(): Done.")
                        break
                    
                    # Timeout may not be an issue if we are purposefully being fed slowly
                    # logging.debug("_work_process_entry(): Timeout on getting item from queue.")

        except BaseException as e:
            reason = "_work_process_entry(): Caught exception.  Tripping breaker and re-raising."
            logging.exception(reason)
            self._breaker.trip(reason)
            raise e
        
        finally:
            if executor:
                logging.debug("_work_process_entry(): Shutting down executor.")
                try:
                    executor.shutdown()
                except BaseException as e:
                    logging.exception("_work_process_entry(): Exception caught during executor shutdown.")

            try:
                logging.debug("_work_process_entry(): Calling _on_work_process_complete().")
                self._on_work_process_complete()
            except BaseException as e:
                logging.exception("_work_process_entry(): _on_work_process_complete() raised exception.")
    
    # This is the entry point for each thread in the thread pools.
    # This simply calls _on_work and trips the breaker on 
    # an exception.
    def _work_thread_entry(self, item):
        try:
            if self._breaker.is_tripped():
                logging.error("_work_thread_entry(): Detected tripped breaker, exiting.")
                return

            self._rate_limiter.wait()
            
            if self._breaker.is_tripped():
                logging.error("_work_thread_entry(): Detected tripped breaker, exiting.")
                return

            self._on_work(item)

        except BaseException as e:
            reason = "_work_thread_entry(): Tripping breaker due to _on_work() exception."
            logging.exception(reason)
            self._breaker.trip(reason)

    #
    # The following methods are all abstract and are defined by the
    # implementor.  No instance of this class can be created without first
    # being derived and implementing these methods.
    #

    # Provides an opportunity for each spawned work process to setup
    # items that are self contained in the spawned worker processes.
    # Since things like database connections and logging do not survive
    # and play well across processes, each work process should set their 
    # own up.  This is their opportunity.  Remember that these items WILL
    # be shared across threads running in the work processes.
    # Should follow the below pattern:
    #
    # 1) Setup logging for the the work process.
    # 2) Connect to databases and external resources.
    # 
    # Remember that EACH of the N work processes will have this called once
    # and self will be a unique copy from the parent and changes will not
    # cross process boundaries.
    #
    def _on_work_process_setup(self):
        pass

    # Provides an opportunity for the spawned acquisition process to setup
    # items that are self contained in the spawned worker processes.
    # Since things like database connections and logging do not survive
    # and play well across processes, each process should set their 
    # own up.  This is their opportunity.  These items should only be used
    # inside the _acquire_work() method and cleaned up in the 
    # _on_acquisition_process_complete() method.
    #
    # 1) Setup logging for the the work process.
    # 2) Connect to databases and external resources.
    # 
    # Remember that EACH of the N work processes will have this called once
    # and self will be a unique copy from the parent and changes will not
    # cross process boundaries.
    #
    def _on_acquisition_process_setup(self):
        pass

    # The implementation's 'main method'.  Acquires the work to be done by all 
    # children.  Called in its own child process.  
    # Should follow the below pattern:
    # 
    # 1) Read chunks of work from database or external resources.
    # 2) For each work item, call _distribute_work.
    # 3) If, for any reason, work should abort, raise an exception to trip the breaker
    #
    @abstractmethod
    def _acquire_work(self):
        pass
    
    # Called when a work item is ready from the queue to be worked by a 
    # work thread inside a work process.  Work the item and return as
    # quickly as possible.  Throw an exception to trip the breaker.
    #
    @abstractmethod
    def _on_work(self, item):
        pass
    
    # Called after the _acquire_work method exits, either due to 
    # clean return or exception.  Use this method to clean-up any resources
    # potentially setup in _acquire_work. 
    #
    def _on_acquisition_process_complete(self):
        pass
    
    # Called after the last thread of a work process exits and the work
    # process can now clean-up anything setup in _on_work_process_setup.
    #
    def _on_work_process_complete(self):
        pass

class UnconfiguredException(Exception):
    pass
