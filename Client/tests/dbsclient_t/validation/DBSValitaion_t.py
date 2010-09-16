"""
DBS3 Validation tests
These tests write and then immediately reads back the data from DBS3 and validate
"""

__revision__ = "$Id: DBSValitaion_t.py,v 1.1 2010/01/29 19:43:58 afaq Exp $"
__version__ = "$Revision $"

import os
import sys
import unittest
from dbs.apis.dbsClient import *
from ctypes import *

def uuid():
    lib = CDLL("libuuid.so.1")
    uuid = create_string_buffer(16)
    return lib.uuid_generate(byref(uuid))

    
url="http://cmssrv18.fnal.gov:8585/dbs3"
api = DbsApi(url=url)
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

class DBSValitaion_t(unittest.TestCase):

    def setUp(self):
        """setup all necessary parameters"""

    def test01(self):
        """test01: web.DBSClientWriter.insertPrimaryDataset: basic test"""
        data = {'primary_ds_name':primary_ds_name,
                'primary_ds_type':'TEST'}
        api.insertPrimaryDataset(primaryDSObj=data)
	primaryList = api.listPrimaryDatasets(primary_ds_name)
	self.assertEqual(len(primaryList), 1)
	for primaryInDBS in primaryList:
	    self.assertEqual(primaryInDBS['primary_ds_name'], primary_ds_name)
	    self.assertEqual(primaryInDBS['primary_ds_type'], 'TEST')


if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSValitaion_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
