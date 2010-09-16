"""
DBS3 Validation tests
These tests write and then immediately reads back the data from DBS3 and validate
"""

__revision__ = "$Id: DBSValitaion_t.py,v 1.2 2010/01/29 21:03:31 afaq Exp $"
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

class DBSValitaion_t(unittest.TestCase):

    def setUp(self):
        """setup all necessary parameters"""

    def test01(self):
        """test01: web.DBSClientWriter.PrimaryDataset: validation test"""
        data = {'primary_ds_name':primary_ds_name,
                'primary_ds_type':'TEST'}
        api.insertPrimaryDataset(primaryDSObj=data)
	primaryList = api.listPrimaryDatasets(primary_ds_name)
	self.assertEqual(len(primaryList), 1)
	primaryInDBS=primaryList[0]
	self.assertEqual(primaryInDBS['primary_ds_name'], primary_ds_name)
	self.assertEqual(primaryInDBS['primary_ds_type'], 'TEST')

    def test02(self):
	"""test02: web.DBSClientWriter.OutputModule: validation test"""
	data = {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name, 'output_module_label': output_module_label}
	api.insertOutputConfig(outputConfigObj=data)
	confList=api.listOutputConfigs(release_version=release_version, pset_hash=pset_hash, \
	                 app_name=app_name, output_module_label=output_module_label)
	self.assertEqual(len(confList), 1)
	confInDBS=confList[0]
	self.assertEqual(confInDBS['release_version'], release_version)
	self.assertEqual(confInDBS['pset_hash'], pset_hash)
	self.assertEqual(confInDBS['app_name'], app_name)
	self.assertEqual(confInDBS['output_module_label'], output_module_label)

    def test03(self):
	"""test03: web.DBSClientWriter.Dataset: validation test"""
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
	dsList = api.listDatasets(dataset=dataset)
	self.assertEqual(len(dsList), 1)
	dsInDBS=dsList[0]
	self.assertEqual(dsInDBS['dataset'], dataset)
	self.assertEqual(dsInDBS['is_dataset_valid'], 1)
	self.assertEqual(dsInDBS['physics_group_name'], 'Tracker')
	self.assertEqual(dsInDBS['dataset_type'], 'PRODUCTION')
	self.assertEqual(dsInDBS['processed_ds_name'], procdataset)
	self.assertEqual(dsInDBS['primary_ds_name'], primary_ds_name)
	self.assertEqual(dsInDBS['release_version'], release_version)
	self.assertEqual(dsInDBS['pset_hash'], pset_hash)
	self.assertEqual(dsInDBS['app_name'], app_name)
	self.assertEqual(dsInDBS['output_module_label'], output_module_label)
	self.assertEqual(dsInDBS['xtcrosssection'], 123)

    def test04(self):
	"""test04 web.DBSClientWriter.Block: validation test"""
	data = {'block_name': block,
		'origin_site': site }
		
	api.insertBlock(blockObj=data)
	blkList = api.listBlocks(block)
	self.assertEqual(len(blkList), 1)
	blkInDBS=blkList[0]
	self.assertEqual(blkInDBS['site_name'], site )
	self.assertEqual(blkInDBS['open_for_writing'], 1)
	self.assertEqual(blkInDBS['dataset'], dataset)
	self.assertEqual(blkInDBS['block_name'], block)
	self.assertEqual(blkInDBS['file_count'], 0)
	self.assertEqual(blkInDBS['block_size'], 0)


    def test05(self):
	"""test05 web.DBSClientWriter.Files: validation test"""
	#
	#    --- NOTICE    _parent    at multiple places below
	# This first part just inserts a parent primary, dataset and block, with parent files
	# That is later used in inserting files in 'block', that are then 'validated'
	pridata = {'primary_ds_name':primary_ds_name+"_parent",
	                    'primary_ds_type':'TEST'}
	api.insertPrimaryDataset(primaryDSObj=pridata)
	data = {
		'is_dataset_valid': 1, 'physics_group_name': 'Tracker', 'dataset': dataset,
	        'dataset_type': 'PRODUCTION', 'processed_ds_name': procdataset+"_parent", 'primary_ds_name': primary_ds_name+"_parent",
		'output_configs': [
		    {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name, 'output_module_label': output_module_label},
		    ],
		'global_tag': u'', 'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': tier,
		'creation_date' : 1234, 'create_by' : 'anzar', "last_modification_date" : 1234, "last_modified_by" : "anzar",
		#'processing_version': '1',  'acquisition_era_name': u'',
		}
	api.insertDataset(datasetObj=data)

	block_parent="/%s/%s/%s#%s" % (primary_ds_name+"_parent", procdataset+"_parent", tier, uid)
	pblkdata = {'block_name': block_parent,
	                    'origin_site': site }
	api.insertBlock(blockObj=pblkdata)
	#parent files
	pflist=[]
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
                'event_count': u'1619',
                'logical_file_name': "/store/mc/parent_%s/%i.root" %(uid, i),
                'block': block_parent
                }
	    pflist.append(f)
	api.insertFiles(filesList={"files":pflist})
	#### This next block of test will now actually insert the files in the "test 'block' in this module, using the upper files as parent
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
                'file_parent_list': [ {"file_parent_lfn" : "/store/mc/parent_%s/%i.root" %(uid, i)} ],
                'event_count': u'1619',
                'logical_file_name': "/store/mc/%s/%i.root" %(uid, i),
                'block': block
			    #'is_file_valid': 1
                }
	    flist.append(f)
	print len(flist)
	print flist
	api.insertFiles(filesList={"files":flist})
	### Lets begin the validation now
	# our block, 'block' now has these 10 files, and that is basis of our validation
	flList=api.listFiles(block=block)
	print flList
	print block

	
	
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSValitaion_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
