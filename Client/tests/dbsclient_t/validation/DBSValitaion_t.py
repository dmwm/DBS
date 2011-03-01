"""
DBS3 Validation tests
These tests write and then immediately reads back the data from DBS3 and validate
"""
import os
import sys
import unittest
from dbs.apis.dbsClient import *
from ctypes import *
import time
import uuid

url=os.environ['DBS_WRITER_URL']    
#url="http://cmssrv18.fnal.gov:8585/dbs3"
api = DbsApi(url=url)
uid = uuid.uuid4().time_mid
print "****uid=%s******" %uid
primary_ds_name = 'unittest_web_primary_ds_name_%s' % uid
procdataset = 'unittest_web_dataset_%s' % uid 
tier = 'GEN-SIM-RAW'
dataset="/%s/%s/%s" % (primary_ds_name, procdataset, tier)
app_name='cmsRun'
output_module_label='Merged'
pset_hash='76e303993a1c2f842159dbfeeed9a0dd%s' %uid 
release_version='CMSSW_1_2_3'
site="cmssrm.fnal.gov"
block="%s#%s" % (dataset, uid)
acquisition_era_name="acq_era_%s" %uid
processing_version="%s" %(uid if (uid<9999) else uid%9999)
flist=[]

class DBSValitaion_t(unittest.TestCase):

    def setUp(self):
        """setup all necessary parameters"""

    def test01(self):
        """test01: web.DBSClientWriter.PrimaryDataset: validation test"""
        data = {'primary_ds_name':primary_ds_name,
                'primary_ds_type':'TEST'}
        #print "data=%s" %data        
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
	"""test03: web.DBSWriterModel.insertAcquisitionEra: Basic test """
	data={'acquisition_era_name': acquisition_era_name}
	api.insertAcquisitionEra(data)

    def test04(self):
	"""test04: web.DBSWriterModel.insertProcessingEra: Basic test """
        data={'processing_version': processing_version, 'description':'this is a test'}
	api.insertProcessingEra(data)
           
    def test05(self):
	"""test05: web.DBSClientWriter.Dataset: validation test"""
	data = {
		'is_dataset_valid': 1, 'physics_group_name': 'Tracker', 'dataset': dataset,
	        'dataset_access_type': 'PRODUCTION', 'processed_ds_name': procdataset, 'primary_ds_name': primary_ds_name,
		'output_configs': [
		    {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name, 'output_module_label': output_module_label},
		    ],
		'global_tag': u'', 'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': tier,
		'creation_date' : 1234, 'create_by' : 'anzar', "last_modification_date" : 1234, "last_modified_by" : "anzar",
		'processing_version': processing_version,  'acquisition_era_name': acquisition_era_name,
		}
        try:
            #print data['dataset']
            api.insertDataset(datasetObj=data)
            #print dataset
            dsList = api.listDatasets(dataset=dataset, detail=True, dataset_access_type='PRODUCTION')
        except Exception, e:
            print e
        #print dsList
	self.assertEqual(len(dsList), 1)
	dsInDBS=dsList[0]
	self.assertEqual(dsInDBS['dataset'], dataset)
	self.assertEqual(dsInDBS['is_dataset_valid'], 1)
	self.assertEqual(dsInDBS['physics_group_name'], 'Tracker')
	self.assertEqual(dsInDBS['dataset_access_type'], 'PRODUCTION')
	self.assertEqual(dsInDBS['processed_ds_name'], procdataset)
	self.assertEqual(dsInDBS['primary_ds_name'], primary_ds_name)
	#self.assertEqual(dsInDBS['release_version'], release_version)
	#self.assertEqual(dsInDBS['pset_hash'], pset_hash)
	#self.assertEqual(dsInDBS['app_name'], app_name)
	#self.assertEqual(dsInDBS['output_module_label'], output_module_label)
	self.assertEqual(dsInDBS['xtcrosssection'], 123)
	self.assertEqual(dsInDBS['processing_version'], processing_version)
	self.assertEqual(dsInDBS['acquisition_era_name'], acquisition_era_name)

    def test06(self):
	"""test06 web.DBSClientWriter.Block: validation test"""
	data = {'block_name': block,
		'origin_site_name': site }
		
	api.insertBlock(blockObj=data)
	blkList = api.listBlocks(block, detail=True)
	self.assertEqual(len(blkList), 1)
	blkInDBS=blkList[0]
	self.assertEqual(blkInDBS['origin_site_name'], site )
	self.assertEqual(blkInDBS['open_for_writing'], 1)
	self.assertEqual(blkInDBS['dataset'], dataset)
	self.assertEqual(blkInDBS['block_name'], block)
	self.assertEqual(blkInDBS['file_count'], 0)
	self.assertEqual(blkInDBS['block_size'], 0)

    def test07(self):
	"""test07 web.DBSClientWriter.Files: validation test"""
	#
	#    --- NOTICE    _parent    at multiple places below
	# This first part just inserts a parent primary, dataset and block, with parent files
	# That is later used in inserting files in 'block', that are then 'validated'
	pridata = {'primary_ds_name':primary_ds_name+"_parent",
	                    'primary_ds_type':'TEST'}
	api.insertPrimaryDataset(primaryDSObj=pridata)
	data = {
		'is_dataset_valid': 1, 'physics_group_name': 'Tracker', 'dataset': dataset,
	        'dataset_access_type': 'PRODUCTION', 'processed_ds_name': procdataset+"_parent", 'primary_ds_name': primary_ds_name+"_parent",
		'output_configs': [
		    {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name, 'output_module_label': output_module_label},
		    ],
		'global_tag': u'', 'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': tier,
		'creation_date' : 1234, 'create_by' : 'anzar', "last_modification_date" : 1234, "last_modified_by" : "anzar",
		'processing_version': processing_version,  'acquisition_era_name': acquisition_era_name,
		}
	api.insertDataset(datasetObj=data)

	block_parent="/%s/%s/%s#%s" % (primary_ds_name+"_parent", procdataset+"_parent", tier, uid)
	pblkdata = {'block_name': block_parent,
	                    'origin_site_name': site }
	api.insertBlock(blockObj=pblkdata)
	#parent files
	pflist=[]
 	for i in range(10):
	    f={  
		'adler32': u'NOTSET', 'file_type': 'EDM',
                'dataset': dataset,
                'file_size': u'201221191', 'auto_cross_section': 0.0, 
                'check_sum': u'1504266448',
                'event_count': u'1619',
                'logical_file_name': "/store/mc/parent_%s/%i.root" %(uid, i),
                'block_name': block_parent,
		'file_lumi_list': [
		                                          {'lumi_section_num': u'27414', 'run_num': u'1'},
		                                      {'lumi_section_num': u'26422', 'run_num': u'1'},
						                                            {'lumi_section_num': u'29838', 'run_num': u'1'}
		                    ]
                }
	    pflist.append(f)
	api.insertFiles(filesList={"files":pflist}, qInserts=False)
	#### This next block of test will now actually insert the files in the "test 'block' in this module, using the upper files as parent
 	for i in range(10):
	    f={  
		'adler32': u'NOTSET', 'file_type': 'EDM',
                'file_output_config_list': 
		    [ 
			{'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name, 'output_module_label': output_module_label},
		    ],
                'dataset': dataset,
                'file_size': u'201221191', 'auto_cross_section': 0.0, 
                'check_sum': u'1504266448',
                'file_lumi_list': [
	                              {'lumi_section_num': u'27414', 'run_num': u'1'},
		                      {'lumi_section_num': u'26422', 'run_num': u'1'},
		                      {'lumi_section_num': u'29838', 'run_num': u'1'}
                                  ],
                'file_parent_list': [ {"file_parent_lfn" : "/store/mc/parent_%s/%i.root" %(uid, i)} ],
                'event_count': u'1619',
                'logical_file_name': "/store/mc/%s/%i.root" %(uid, i),
                'block_name': block
			    #'is_file_valid': 1
                }
	    flist.append(f)
	#import pdb
        api.insertFiles(filesList={"files":flist}, qInserts=False)
        #pdb.set_trace()

	### Lets begin the validation now
	# our block, 'block' now has these 10 files, and that is basis of our validation
	flList=api.listFiles(block=block, detail=True)
	self.assertEqual(len(flList), 10)
	for afileInDBS in flList:
	    self.assertEqual(afileInDBS['block_name'], block)
	    self.assertEqual(afileInDBS['event_count'], 1619)
	    self.assertEqual(afileInDBS['file_size'], 201221191)
	    self.assertEqual(afileInDBS['is_file_valid'], 1)
	# Get the file parent -- The inserted file must have a parent
        logical_file_name = "/store/mc/%s/%i.root" %(uid, 0)
	flParentList=api.listFileParents(logical_file_name=logical_file_name)
	self.assertEqual(len(flParentList), 1)
	self.assertEqual(flParentList[0][logical_file_name][0], "/store/mc/parent_%s/%i.root" %(uid, 0))
	# Get the dataset parent -- due to fact that files had parents, dataset parentage is also inserted
	dsParentList=api.listDatasetParents(dataset=dataset)
	self.assertEqual(len(dsParentList), 1)
	self.assertEqual(dsParentList[0]['parent_dataset'], "/%s/%s/%s" % (primary_ds_name+"_parent", procdataset+"_parent", tier) )
	# block parameters, such as file_count must also be updated, lets validate
    	blkList = api.listBlocks(block, detail=True)
	self.assertEqual(len(blkList), 1)
	blkInDBS=blkList[0]
	self.assertEqual(blkInDBS['origin_site_name'], site )
	self.assertEqual(blkInDBS['open_for_writing'], 1)
	self.assertEqual(blkInDBS['dataset'], dataset)
	self.assertEqual(blkInDBS['block_name'], block)
	# 10 files
	self.assertEqual(blkInDBS['file_count'], 10)
	# size should be 10 X 2012211901 (file_size) = 2012211910
	self.assertEqual(blkInDBS['block_size'], 2012211910)

    def test08(self):
	"""update file status and validate that it got updated"""
	logical_file_name = "/store/mc/%s/%i.root" %(uid, 0)
	#print "WARNING : DBS cannot list INVALID file, so for now this test is commented out"
	api.updateFileStatus(logical_file_name=logical_file_name, is_file_valid=0)
	#listfile
	filesInDBS=api.listFiles(logical_file_name=logical_file_name, detail=True)
	self.assertEqual(len(filesInDBS) ,1)
	self.assertEqual(filesInDBS[0]['is_file_valid'], 0)
	
    def test09(self):
	"""test09 web.DBSClientWriter.updateDatasetStatus: should be able to update dataset status and validate it"""
	api.updateDatasetStatus(dataset=dataset, is_dataset_valid=1)
	dsInDBS=api.listDatasets(dataset=dataset,  dataset_access_type="PRODUCTION", detail=True)
	self.assertEqual(len(dsInDBS), 1)
	self.assertEqual(dsInDBS[0]['is_dataset_valid'], 1)
	
    def test10(self):
	"""test10 web.DBSClientWriter.updateDatasetType: should be able to update dataset type"""
	api.updateDatasetType(dataset=dataset, dataset_access_type="READONLY")
	dsInDBS=api.listDatasets(dataset=dataset, detail=True)
        self.assertEqual(len(dsInDBS), 1)
	self.assertEqual(dsInDBS[0]['dataset_access_type'], "READONLY")


	
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSValitaion_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
