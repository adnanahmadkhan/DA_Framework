#!/usr/bin/python3
import logging
import datetime
import json
import time
import argparse
from pymongo import MongoClient, ReplaceOne
from collections import OrderedDict
from time import sleep

import mysql.connector
from sqlalchemy import create_engine

from afiniti_da import MultiprocessingThreadingWorkQueue, BreakerTrippedException, ThreadingAccumulator


# Our "bulk writer".  Construct with size in the on_work_process_setup()
# method of our MyQueue class.
#
class MyAccumulator(ThreadingAccumulator):
    def __init__(self, size, target):
        super().__init__(size)
        self._target_collection = target

    def _on_process(self, the_list):
        # self._target_collection.insert_many(the_list)
        # logging.info("Accumulator processing list: " + str(the_list))
        self._target_collection.bulk_write(the_list, ordered=False)


class MyQueue(MultiprocessingThreadingWorkQueue):
    
    # Our constructor simply takes the json config file dictionary
    # to save as class attribute then calls the base constructor with
    # all the defaults.  The base class configuration will be taken
    # from the argparse() call below.
    #
    def __init__(self, config, log_file_fmt):

        super().__init__()
        self._config = config
        self._log_file_fmt = log_file_fmt

    # Setup OUR additional arguments for the argparse and pass off to base 
    # class to parse common arguments that control the work queue config.
    #
    def argparse(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--bulk-write", default=10000, type=int, help="number of records per bulk write")
        args = super().argparse(parser)
        self._bulk_write = int(args.bulk_write)
    
    # Setup NEW logging and database for the acquisition process.
    #
    def _on_acquisition_process_setup(self):
        logging.basicConfig(filename=self._log_file_fmt,
            filemode='a',
            level=logging.INFO,
            format="[%(asctime)s.%(msecs)03d][%(process)d][%(thread)d][acquire][%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S")
        
        da_mongo_client = MongoClient(host=self._config["host"], port=self._config["port"], username=self._config["username"], password=self._config["password"],authMechanism=self._config["authMechanism"])

        self._btns_collection = da_mongo_client["DA_EKATA"]["test_input"]
    
    # Setup NEW logging, database, http sessions, bulk writer for the 
    # work processes.
    #
    def _on_work_process_setup(self):
        logging.basicConfig(filename=self._log_file_fmt,
            filemode='a',
            level=logging.INFO,
            format="[%(asctime)s.%(msecs)03d][%(process)d][%(thread)d][work][%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S")
        
        mongo_client = MongoClient(host=self._config["host"], port=self._config["port"], username=self._config["username"], password=self._config["password"],authMechanism=self._config["authMechanism"])
        
        self._lookup = mysql.connector.connect(
            user=self._config["sql_username"], 
            password=self._config["sql_password"], 
            host=self._config["host"],
            database='US_PORT_DATA_V2')
        # output collection
        target_collection = mongo_client["DA_EKATA"]["test_output"]
        
        self._accumulator = MyAccumulator(size=self._bulk_write, target=target_collection)
    
    # The main "read loop" that feeds all the workers.
    #
    def _acquire_work(self):
        for doc in self._btns_collection.find():
            self._distribute_work(doc)

    # Code to handle each work item as it comes into a thread in a 
    # work process.
    #
    def _on_work(self, item):
        item = self.logic(item)
        self._accumulator.add(ReplaceOne({'_id': item["_id"]}, item, upsert=True))

    def logic(self, record):
        try:
            btn = record["_id"]
            query = f"SELECT * FROM PORT_DATA_GRF_US_HIST WHERE DialCodeOrPrefix = '{btn}' ORDER BY PortDateTime desc LIMIT 1;"
            date_now = datetime.datetime(2021,5,1)
            
            # trying to get connection
            cursor = self._lookup.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
            if row:
                portdate = row[7]
                delta = date_now - portdate
                delta = delta.total_seconds()
                delta /= 2592000 # divide by total seconds in 1 month
                record["LINE_TENURE_MONTH_2"] = round(delta)
            else:
                record["LINE_TENURE_MONTH_2"] = 12345

            return record
            
        except BreakerTrippedException as e:
            logging.error("Breaker was tripped for reason: " + str(e.reason))

    
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

    system_time = datetime.datetime.now().strftime("%m_%d_%Y_%H_%M_%S")
    print("Script begins at ", system_time)
    start_time = time.time()
    log_file_fmt = "logs/log_" + str(system_time) + ".log"  # %s = process type, %d = pid
    
    with open("config.json") as config_json:
        config = json.load(config_json)

    mq = MyQueue(config=config, log_file_fmt=log_file_fmt)

    try:
        mq.argparse()
        mq.run()

    except BreakerTrippedException as e:
        print("Breaker was tripped for reason: " + str(e.reason))

    print("Done & Dusted --- %s seconds ---" % (time.time() - start_time))

# --bulk-write 500 --processes 8 --threads 25 --queue 1000 --rate 100
