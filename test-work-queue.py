#!/usr/bin/python3

import logging
import time
import argparse


from afiniti_da import MultiprocessingThreadingWorkQueue, BreakerTrippedException, ThreadingAccumulator


# Our "bulk writer".  Construct with size in the on_work_process_setup()
# method of our MyQueue class.
#
class MyAccumulator(ThreadingAccumulator):
    def __init__(self, size):
        super().__init__(size)

    def _on_process(self, the_list):
        logging.info("Accumulator processing list: " + str(the_list))


class MyQueue(MultiprocessingThreadingWorkQueue):
    
    # Note that because we do not declare __init__(), we use the default base
    # constructor.

    # Setup OUR additional arguments for the argparse and pass off to base 
    # class to parse common arguments that control the work queue config.
    #
    def argparse(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--bulk-write", default=13, type=int, help="number of records per bulk write")
        args = super().argparse(parser)
        self._bulk_write = int(args.bulk_write)
    
    # Setup NEW logging and database for the acquisition process.
    #
    def _on_acquisition_process_setup(self):
        logging.basicConfig(
            level=logging.DEBUG,
            format="[%(asctime)s.%(msecs)03d][%(process)d][%(thread)d][acquire][%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S")
    
    # Setup NEW logging, database, http sessions, bulk writer for the 
    # work processes.
    #
    def _on_work_process_setup(self):
        logging.basicConfig(
            level=logging.DEBUG,
            format="[%(asctime)s.%(msecs)03d][%(process)d][%(thread)d][work][%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S")
        
        self._accumulator = MyAccumulator(size=self._bulk_write)
    
    # The main "read loop" that feeds all the workers.
    #
    def _acquire_work(self):

        for i in range(100):
            logging.info("Distribute " + str(i))
            self._distribute_work(i)
    
    # Code to handle each work item as it comes into a thread in a 
    # work process.
    #
    def _on_work(self, item):
        logging.info("Got item " + str(item))
        self._accumulator.add(item)
    
    # Code that runs after the acquisition process completes.  
    # Perform any needed cleanup and/or logging.
    #
    def _on_acquisition_process_complete(self):
        logging.info("acquisition process complete")
    
    # Code that runs after each work process completes after
    # the thread pools shut down.  Perform any needed cleanup or
    # logging.
    # 
    def _on_work_process_complete(self):
        logging.info("Flushing accumulator.")
        self._accumulator.flush()
        logging.info("Work process complete.")


# Mainline here...
#
if __name__ == "__main__":

    mq = MyQueue()

    try:
        mq.argparse()
        mq.run()

    except BreakerTrippedException as e:
        print("Breaker was tripped for reason: " + str(e.reason))

    print("Completed.")
