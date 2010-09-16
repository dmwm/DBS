"""This module provides all basic tests for the server"""

__revision__ = "$Id: TestListTime.py,v 1.1 2009/11/26 16:49:42 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"

import threading
from DBS3SimpleClient import DBS3Client
from threading import Thread
import time
import cjson

class DBS3StressTest(Thread):
    def __init__(self, IC, url = "http://localhost:8585/dbs3/"):
        Thread.__init__(self)
        self.cli = DBS3Client(url)
        self.IC = IC
        
    def run(self):
        """insert one Primary Dataset, one Dataset, one Block, 
        nfiles Files and one file with nfiles parents and nlumis lumi sections"""
        #listDatasets
        t = time.time()
        self.cli.get("datasets")
        print "%s: LIST DATASETS:    %s" % (self.IC, time.time() - t)
        
if __name__ == "__main__":
    for i in range(0, 100):
        current = DBS3StressTest(url = "http://localhost:8585/dbs3/", IC = i)
        #current = DBS3StressTest(url="http://vocms09.cern.ch:8989/DBSServlet/", IC = i)
        current.start()
