"""
client writer unittests
"""

__revision__ = "$Id: DBSClientWriter_t.py,v 1.2 2010/01/25 17:15:45 afaq Exp $"
__version__ = "$Revision: 1.2 $"

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

class DBSClientWriter_t(unittest.TestCase):

    def setUp(self):
        """setup all necessary parameters"""

    def test01(self):
        """test01: web.DBSClientWriter.insertPrimaryDataset: basic test"""
        data = {'primary_ds_name':primary_ds_name,
                'primary_ds_type':'TEST'}
        api.insertPrimaryDataset(primaryDSObj=data)

    def test02(self):
        """test02: web.DBSClientWriter.insertPrimaryDataset: duplicate should not riase an exception"""
        data = {'primary_ds_name':primary_ds_name,
                'primary_ds_type':'TEST'}
        api.insertPrimaryDataset(primaryDSObj=data)
	
    def test04(self):
	"""test04: web.DBSClientWriter.insertOutputModule: basic test"""
	data = {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name, 'output_module_label': output_module_label}
	api.insertOutputConfig(outputConfigObj=data)

    def test05(self):
        """test05: web.DBSClientWriter.insertOutputModule: re-insertion should not raise any errors"""
        data = {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name, 'output_module_label': output_module_label}
        api.insertOutputConfig(outputConfigObj=data)

    def test07(self):
	"""test07: web.DBSClientWriter.insertDataset: basic test"""
	data = {
		'is_dataset_valid': 1, 'physics_group_name': 'Tracker', 'dataset': dataset,
	        'dataset_type': 'PRODUCTION', 'processed_ds_name': procdataset, 'primary_ds_name': primary_ds_name,
		'output_configs': [
		    {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name, 'output_module_label': output_module_label},
		    ],
		'global_tag': u'', 'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': tier,
		'creation_date' : 1234, 'create_by' : 'anzar', "last_modification_date" : 1234, "last_modified_by" : "anzar",
		#'processing_version': '1',  'acquisition_era_name': u'',
		}
	api.insertDataset(datasetObj=data)
	
    def test08(self):
	"""test08: web.DBSClientWriter.insertDataset: duplicate insert should be ignored"""
	data = {
		'is_dataset_valid': 1, 'physics_group_name': 'Tracker', 'dataset': dataset,
	        'dataset_type': 'PRODUCTION', 'processed_ds_name': procdataset, 'primary_ds_name': primary_ds_name,
		'output_configs': [
		    {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name, 'output_module_label': output_module_label},
		], 
		'global_tag': u'', 'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': tier,
		'creation_date' : 1234, 'create_by' : 'anzar', "last_modification_date" : 1234, "last_modified_by" : "anzar",
		#'processing_version': '1',  'acquisition_era_name': u'',
		}
	
	api.insertDataset(datasetObj=data)

    def test11(self):
	"""test11: web.DBSClientWriter.insertDataset: no output_configs, should be fine insert!"""
	data = {
		'dataset': dataset,
		'is_dataset_valid': 1, 'physics_group_name': 'Tracker', 'primary_ds_name': primary_ds_name,
	        'dataset_type': 'PRODUCTION', 'processed_ds_name': procdataset,
		'global_tag': u'', 'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': tier,
		'creation_date' : 1234, 'create_by' : 'anzar', "last_modification_date" : 1234, "last_modified_by" : "anzar",
		#'processing_version': '1',  'acquisition_era_name': u'',
		}
	api.insertDataset(datasetObj=data)

    def test12(self):
	"""test12: web.DBSClientWriter.insertSite: basic test"""
	data = {
	     "site_name" : site
	}
	api.insertSite(siteObj=data)


    def test13(self):
	"""test13: web.DBSClientWriter.insertSite: duplicate site must not throw any errors"""
	data = {
	     "site_name" : site
	}
        api.insertSite(siteObj=data)
	
    def test14(self):
	"""test14 web.DBSClientWriter.insertBlock: basic test"""
	data = {'block_name': block,
		'origin_site': site }
		
	api.insertBlock(blockObj=data)

    def test14(self):
	"""test14 web.DBSClientWriter.insertBlock: duplicate insert should not raise exception"""
	data = {'block_name': block,
		'origin_site': site }
		
	api.insertBlock(blockObj=data)

    def test15(self):
	"""test15 web.DBSClientWriter.insertFiles: basic test"""
	
	flist=[]
 	for i in range(10):
	    f={  
		'adler32': u'NOTSET', 'file_type': 'EDM',
                'file_output_config_list': 
		    [ 
			{'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name, 'output_module_label': output_module_label},
		    ],
                'dataset': dataset,
                'file_size': u'2012211901', 'auto_cross_section': 0.0, 
                'check_sum': u'1504266448',
                'file_lumi_list': [
	                              {'lumi_section_num': u'27414', 'run_num': u'1'},
		                      {'lumi_section_num': u'26422', 'run_num': u'1'},
		                      {'lumi_section_num': u'29838', 'run_num': u'1'}
                                  ],
                'file_parent_list': [ ],
                'event_count': u'1619',
                'logical_file_name': "/store/mc/%i.root" %i,
                'block': block
			    #'is_file_valid': 1
                }
	    flist.append(f)
	api.insertFiles(filesList={"files":flist})

    def test16(self):
	"""test16 web.DBSClientWriter.insertFiles: duplicate insert file shuld not raise any errors"""
	flist=[]
 	for i in range(10):
	    f={  
		'adler32': u'NOTSET', 'file_type': 'EDM',
                'file_output_config_list': 
		    [ 
			{'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name, 'output_module_label': output_module_label},
		    ],
                'dataset': dataset,
                'file_size': u'2012211901', 'auto_cross_section': 0.0, 
                'check_sum': u'1504266448',
                'file_lumi_list': [
	                              {'lumi_section_num': u'27414', 'run_num': u'1'},
		                      {'lumi_section_num': u'26422', 'run_num': u'1'},
		                      {'lumi_section_num': u'29838', 'run_num': u'1'}
                                  ],
                'file_parent_list': [ ],
                'event_count': u'1619',
                'logical_file_name': "/store/mc/%i.root" %i,
                'block': block
			    #'is_file_valid': 1
                }
	    flist.append(f)
	api.insertFiles(filesList={"files":flist})
 
   
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSClientWriter_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
