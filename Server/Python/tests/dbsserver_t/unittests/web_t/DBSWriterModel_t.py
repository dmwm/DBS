"""
web unittests
"""

__revision__ = "$Id: DBSWriterModel_t.py,v 1.10 2010/01/22 15:48:57 yuyi Exp $"
__version__ = "$Revision: 1.10 $"

import logging
import os
import sys
import unittest
from sqlalchemy.exceptions import IntegrityError 
from dbsserver_t.utils.DBSRestApi import DBSRestApi
from ctypes import *

logging.basicConfig(level=logging.ERROR)

class NullDevice:
    def write(self, s):
	pass

"""
This is has to be change everytime running the test. So we need to make it using uuid. YG 1/11/10
COUNTER = os.environ['DBS_TEST_COUNTER']
"""

def uuid():
    lib = CDLL("libuuid.so.1")
    uuid = create_string_buffer(16)
    return lib.uuid_generate(byref(uuid))
    
"""	
config = os.environ["DBS_TEST_CONFIG_WRITER"] 
api = DBSRestApi(config) 
uid = uuid()
primary_ds_name = 'unittest_web_primary_ds_name_%s' % uid
procdataset = 'unittest_web_dataset_%s' % uid 
tier = 'GEN-SIM-RAW'
dataset="/%s/%s/%s" % (primary_ds_name, procdataset, tier)
app_name='cmsRun'
output_module_label='Merged'
pset_hash='76e303993a1c2f842159dbfeeed9a0dd' 
release_version='CMSSW_1_2_3'
site="cmssrm.fnal.gov"
block="%s#%s" % (dataset, uid)
flist=[]
"""
class DBSWriterModel_t(unittest.TestCase):

    def setUp(self):
        """setup all necessary parameters"""

    def atest01(self):
        """test01: web.DBSWriterModel.insertPrimaryDataset: basic test"""
        data = {'primary_ds_name':primary_ds_name,
                'primary_ds_type':'TEST'}
        api.insert('primarydatasets', data)
   
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSWriterModel_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
