import os
import time
import logging
import threading
from WMCore.Database.DBFactory import DBFactory
from dbs.business.DBSFile import DBSFile

class DBS3BusinessList(threading.Thread):
    def __init__(self, dburl, owner):
        threading.Thread.__init__(self) 
        logger = logging.getLogger("dbs test logger")
        dbi = DBFactory(logger, dburl).connect()
        self.bo = DBSFile(logger, dbi, owner)

    def run(self):
        t = time.time()
        print self.bo.listFiles("/GlobalAug07-C/Online/RAW")
        print "Time: %s " % (time.time() - t)

if __name__ == "__main__":
    for i in range(50):
	dburl = os.environ["DBS_TEST_DBURL_READER"]
	dbowner = os.environ["DBS_TEST_DBOWNER_READER"]
	current = DBS3BusinessList(dburl, dbowner) 
	current.start()


