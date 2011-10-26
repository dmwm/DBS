"""
client writer unittests
"""
import os, sys, imp
import time
import uuid
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

uid = uuid.uuid4().time_mid
print "****uid=%s******" %uid

class DBSClientBlockWriter_t(unittest.TestCase):

    def setUp(self):
        """setup all necessary parameters"""
	
    def test1000(self):
	"""test1000 web.DBSClientWriter.insertBlockBulk: basic test"""
        testparams['dataset_conf_list'][0]['app_name'] = "%s_%s"%(testparams['dataset_conf_list'][0]['app_name'], uid)
        for i in range(len(testparams['file_conf_list'])):
            testparams['file_conf_list'][i]['app_name'] = "%s_%s"%(testparams['file_conf_list'][i]['app_name'], uid)
            testparams['file_conf_list'][i]['lfn'] =  "%s_%s" %(testparams['file_conf_list'][i]['lfn'],uid)

        for k in range(len(testparams['files'])):
             testparams['files'][k]['logical_file_name'] = "%s_%s" %(testparams['files'][k]['logical_file_name'], uid)

        testparams['primds']['primary_ds_name'] ='%s_%s' %(testparams['primds']['primary_ds_name'], uid)

        testparams['dataset']['dataset'] = '%s_%s' %(testparams['dataset']['dataset'],uid)

        testparams['block']['block_name'] = '%s_%s' %(testparams['block']['block_name'],uid)
        #print  testparams
	api.insertBulkBlock(blockDump=testparams)
	# insert the parent block as well

if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSClientBlockWriter_t)
    infofile=open("blockdump.dict","r")
    testparams=importCode(infofile, "testparams", 0).blockDump

    unittest.TextTestRunner(verbosity=2).run(SUITE)
else:
    testparams={}


