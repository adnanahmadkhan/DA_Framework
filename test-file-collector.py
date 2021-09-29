#!/usr/bin/python3

import logging
import time
import argparse

from afiniti_da import MultiprocessingFileCollector

# Mainline here...
#
if __name__ == "__main__":
    
    with open(file="file-collector-test-output.txt", mode="w", encoding="utf-8") as f:
        c = MultiprocessingFileCollector(openedFile=f)
        c.start()
        c.collect("hey\n")
        c.collect("hey\n")
        c.collect("we're the monkees\n")
        c.join()
