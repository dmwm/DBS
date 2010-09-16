"""This module provides all basic tests for the server"""

__revision__ = "$Id: TestListTime.py,v 1.2 2009/11/29 11:37:54 akhukhun Exp $"
__version__ = "$Revision: 1.2 $"

import threading
from DBS3SimpleClient import DBS3Client
from threading import Thread
import time
import cjson

class DBS3ListTest(Thread):
    def __init__(self, IC, url = "http://localhost:8585/dbs3/"):
        Thread.__init__(self)
        self.cli = DBS3Client(url)
        self.IC = IC
        
    def run(self):
        """insert one Primary Dataset, one Dataset, one Block, 
        nfiles Files and one file with nfiles parents and nlumis lumi sections"""
    
        #listDatasets
        t = time.time()
        res = self.cli.get("datasets")
        datasets = cjson.decode(res)["result"]
        for i in range(20):
            d = datasets[i]
            files = self.cli.get("files?dataset=%s" % d["DATASET"])
            print "TEST: %s,  DATASET: %s, Time: %s " % (self.IC, d["DATASET"], time.time() - t)
        #print "%s: LIST DATASETS:    %s" % (self.IC, time.time() - t)
        
        
if __name__ == "__main__":
    for i in range(0, 1):
        #current = DBS3ListTest(url = "http://localhost/intlxx/", IC = i)
        current = DBS3ListTest(url = "http://localhost/intlyy/", IC = i)
        #current = DBS3ListTest(url="http://vocms09.cern.ch:8989/DBSServlet/", IC = i)
        current.start()
