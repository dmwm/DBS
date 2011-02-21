"""
client writer unittests
"""
import os, sys, imp
import time
import unittest
from dbs.apis.dbsClient import *
from ctypes import *
    
url=os.environ['DBS_WRITER_URL']     
#url="http://cmssrv18.fnal.gov:8585/dbs3"
api = DbsApi(url=url)
print url

def importCode(code,name,add_to_sys_modules=0):
    module = imp.new_module(name)
    exec code in module.__dict__
    if add_to_sys_modules:
        sys.modules[name] = module
    return module

class DBSClientBlockWriter_t(unittest.TestCase):

    def setUp(self):
        """setup all necessary parameters"""
	
    def test1000(self):
	"""test1000 web.DBSClientWriter.insertBlockBluk: basic test"""
        #print  testparams
	api.insertBlockBluk(blockDump=testparams)
	# insert the parent block as well

if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSClientWriter_t)
    infofile=open("blockdump.dict","r")
    testparams=importCode(infofile, "testparams", 0).blockDump

    unittest.TextTestRunner(verbosity=2).run(SUITE)
else:
    testparams={}


