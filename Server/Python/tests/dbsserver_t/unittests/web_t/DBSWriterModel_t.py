"""
web unittests
"""

__revision__ = "$Id: DBSWriterModel_t.py,v 1.5 2010/01/14 20:09:21 afaq Exp $"
__version__ = "$Revision: 1.5 $"

import os
import unittest
from dbsserver_t.utils.DBSRestApi import DBSRestApi
from ctypes import *

"""
This is has to be change everytime running the test. So we need to make it using uuid. YG 1/11/10
COUNTER = os.environ['DBS_TEST_COUNTER']
"""
class DBSWriterModel_t(unittest.TestCase):

    def uuid(self):
	lib = CDLL("libuuid.so.1")
	uuid = create_string_buffer(16)
	return lib.uuid_generate(byref(uuid))
	
    def setUp(self):
        """setup all necessary parameters"""
        config = os.environ["DBS_TEST_CONFIG_WRITER"] 
        self.api = DBSRestApi(config) 
	self.uid = self.uuid()
	self.primary_ds_name = 'unittest_web_primary_ds_name_%s' % self.uid
	self.dataset = 'unittest_web_dataset_%s' % self.uid 
	self.tier = 'GEN-SIM-RAW'
	self.app_name='cmsRun'
	self.output_module_label='Merged'
	self.pset_hash='76e303993a1c2f842159dbfeeed9a0dd' 
	self.release_version='CMSSW_1_2_3'
    

    def test01(self):
        """web.DBSReaderModel.insertPrimaryDataset: basic test"""
	COUNTER = self.uuid()
	#import pdb
	#pdb.set_trace()
        data = {'primary_ds_name':self.primary_ds_name,
                'primary_ds_type':'TEST'}
        self.api.insert('primarydatasets', data)

    def test02(self):
        """web.DBSReaderModel.insertPrimaryDataset: duplicate should noyt riase an exception"""
        #import pdb
        #pdb.set_trace()
        data = {'primary_ds_name':self.primary_ds_name,
                'dataset':self.dataset}
        self.api.insert('primarydatasets', data)
	
    def test03(self):
	"""web.DBSReaderModel.insertPrimaryDataset: missing 'primary_ds_name, must throw exception"""
	data = {'primary_ds_type':'TEST'}
	try:
	    self.api.insert('primarydatasets', data)
	except:
	     pass
	else:
	     self.fail("Exception was expected and was not raised.")

    def test04(self):
	"""web.DBSReaderModel.insertOutputModule: basic test"""
	data = {'release_version': self.release_version, 'pset_hash': self.pset_hash, 'app_name': self.app_name, 'output_module_label': self.output_module_label}
	self.api.insert('outputconfigs', data)

    def test05(self):
        """web.DBSReaderModel.insertOutputModule: re-insertion should not raise any errors"""
        data = {'release_version': self.release_version, 'pset_hash': self.pset_hash, 'app_name': self.app_name, 'output_module_label': self.output_module_label}
        self.api.insert('OutputConfig', data)
	
    def test06(self):
	"""web.DBSReaderModel.insertOutputModule: missing parameter must cause an exception"""
	data = {'pset_hash': self.pset_hash, 'app_name': self.app_name, 'output_module_label': self.output_module_label}
	try:
	    self.api.insert('outputconfigs', data)
	except:
	    pass
	else:
	    self.fail("Exception was expected and was not raised.")
	    
    def test07(self):
	"""web.DBSReaderModel.insertDataset: basic test"""
	data = {
		'is_dataset_valid': 1, 'physics_group_name': 'Tracker', 'dataset': self.dataset,
	        'dataset_type': 'PRODUCTION', 'processed_ds_name': 'DBS3_Test_2010', 'primary_ds_name': self.primary_ds_name,
		'output_configs': [
		    {'release_version': self.release_version, 'pset_hash': self.pset_hash, 'app_name': self.app_name, 'output_module_label': self.output_module_label},
		    ],
		'global_tag': u'', 'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': self.tier,
		'creation_date' : 1234, 'create_by' : 'anzar', "last_modification_date" : 1234, "last_modified_by" : "anzar",
		#'processing_version': '1',  'acquisition_era_name': u'',
		}
	self.api.insert('datasets', data)
	
    def test08(self):
	"""web.DBSReaderModel.insertDataset: duplicate insert should be ignored"""
	data = {
		'is_dataset_valid': 1, 'physics_group_name': 'Tracker', 'dataset': self.dataset,
	        'dataset_type': 'PRODUCTION', 'processed_ds_name': 'DBS3_Test_2010', 'primary_ds_name': self.primary_ds_name,
		'output_configs': [
		    {'release_version': self.release_version, 'pset_hash': self.pset_hash, 'app_name': self.app_name, 'output_module_label': self.output_module_label},
		], 
		'global_tag': u'', 'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': self.tier,
		'creation_date' : 1234, 'create_by' : 'anzar', "last_modification_date" : 1234, "last_modified_by" : "anzar",
		#'processing_version': '1',  'acquisition_era_name': u'',
		}
	self.api.insert('datasets', data)
	
    def test09(self):
	"""web.DBSReaderModel.insertDataset: missing primary dataset must raise an error"""
	data = {
		'is_dataset_valid': 1, 'physics_group_name': 'Tracker', 'dataset': self.dataset,
	        'dataset_type': 'PRODUCTION', 'processed_ds_name': 'DBS3_Test_2010',
		'output_configs': [
		    {'release_version': self.release_version, 'pset_hash': self.pset_hash, 'app_name': self.app_name, 'output_module_label': self.output_module_label},
		],
		'global_tag': u'', 'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': self.tier,
		'creation_date' : 1234, 'create_by' : 'anzar', "last_modification_date" : 1234, "last_modified_by" : "anzar",
		#'processing_version': '1',  'acquisition_era_name': u'',
		}
	try:
	    self.api.insert('datasets', data)
	except:
	    pass
	else:
	    self.fail("Exception was expected and was not raised.")
	    
    def test10(self):
	"""web.DBSReaderModel.insertDataset: missing parameter must raise an error"""
	data = {
		'is_dataset_valid': 1, 'physics_group_name': 'Tracker', 'primary_ds_name': self.primary_ds_name,
	        'dataset_type': 'PRODUCTION', 'processed_ds_name': 'DBS3_Test_2010',
		'output_configs': [
		    {'release_version': self.release_version, 'pset_hash': self.pset_hash, 'app_name': self.app_name, 'output_module_label': self.output_module_label},
		],
		'global_tag': u'', 'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': self.tier,
		'creation_date' : 1234, 'create_by' : 'anzar', "last_modification_date" : 1234, "last_modified_by" : "anzar",
		#'processing_version': '1',  'acquisition_era_name': u'',
		}
	try:
	    self.api.insert('datasets', data)
	except:
	    pass
	else:
	    self.fail("Exception was expected and was not raised.")
	    
    def test11(self):
	"""web.DBSReaderModel.insertDataset: no output_configs, should be fine insert!"""
	data = {
		'dataset': self.dataset+"_nocfg",
		'is_dataset_valid': 1, 'physics_group_name': 'Tracker', 'primary_ds_name': self.primary_ds_name,
	        'dataset_type': 'PRODUCTION', 'processed_ds_name': 'DBS3_Test_2010',
		'global_tag': u'', 'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': self.tier,
		'creation_date' : 1234, 'create_by' : 'anzar', "last_modification_date" : 1234, "last_modified_by" : "anzar",
		#'processing_version': '1',  'acquisition_era_name': u'',
		}
	self.api.insert('datasets', data)
 
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSWriterModel_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
