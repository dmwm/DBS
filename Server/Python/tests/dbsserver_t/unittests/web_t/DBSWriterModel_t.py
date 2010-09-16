"""
web unittests
"""

__revision__ = "$Id: DBSWriterModel_t.py,v 1.27 2010/08/24 19:48:44 yuyi Exp $"
__version__ = "$Revision: 1.27 $"

import os
import sys
import unittest
import time
#import uuid
from ctypes import *
from dbsserver_t.utils.DBSRestApi import DBSRestApi

class NullDevice:
    def write(self, s):
        pass

def uuid():
    lib = CDLL("libuuid.so.1")
    uuid = create_string_buffer(16)
    return lib.uuid_generate(byref(uuid))

config = os.environ["DBS_TEST_CONFIG_WRITER"] 
service = os.environ["DBS_TEST_SERVICE"] 
api = DBSRestApi(config, service)
uid = uuid()
# cannot use python uuid since the generated uuid is too big to fit into some db columns.
#uid = int(uuid.uuid1())
#uid = str(uuid.uuid1())
print "****uid=%s******" %uid
primary_ds_name = 'unittest_web_primary_ds_name_%s' % uid
procdataset = 'unittest_web_dataset_%s' % uid 
tier = 'GEN-SIM-RAW'
dataset="/%s/%s/%s" % (primary_ds_name, procdataset, tier)
child_dataset="/%s/child_%s/%s" % (primary_ds_name, procdataset, tier)
app_name='cmsRun'
output_module_label='Merged-%s' %uid
pset_hash='76e303993a1c2f842159dbfeeed9a0dd' 
release_version='CMSSW_1_2_%s' % uid
site="cmssrm-%s.fnal.gov" %uid
block="%s#%s" % (dataset, uid)
child_block="%s#%s" % (child_dataset, uid)
acquisition_era_name="acq_era_%s" %uid
processing_version="%s" %(uid if (uid<9999) else uid%9999)
run_num=uid
flist=[]
#print "acquisition_era_name=%s" %acquisition_era_name
#print "processing_version=%s" %processing_version
primary_ds_type='TEST'

outDict={
"primary_ds_name" : primary_ds_name,
"procdataset" : procdataset,
"tier" : tier,
"dataset" : dataset,
"child_dataset" : child_dataset,
"app_name" : app_name,
"output_module_label" : output_module_label,
"pset_hash" : pset_hash,
"release_version" : release_version,
"site" : site,
"block" : block,
"child_block" : child_block,
"files" : [],
"parent_files" : [],
"run_num":run_num,
"acquisition_era":acquisition_era_name,
"processing_version" : processing_version,
"primary_ds_type" : primary_ds_type
}

class DBSWriterModel_t(unittest.TestCase):

    def setUp(self):
        """setup all necessary parameters"""

    def test01(self):
        """test01: web.DBSWriterModel.insertPrimaryDataset: basic test"""
        data = {'primary_ds_name':primary_ds_name,
                'primary_ds_type':primary_ds_type}
        api.insert('primarydatasets', data)

    def test02(self):
        """test02: web.DBSWriterModel.insertPrimaryDataset: duplicate should not riase an exception"""
        data = {'primary_ds_name':primary_ds_name,
                'primary_ds_type':primary_ds_type}
        api.insert('primarydatasets', data)
	
    def test03(self):
	"""test03: web.DBSWriterModel.insertPrimaryDataset: missing primary_ds_name, must throw exception"""
	#import pdb
	#pdb.set_trace()
		
	data = {'primary_ds_type':primary_ds_type}
	try:
	    junk = api.insert('primarydatasets', data)
	except Exception, ex:
	    #print "**** "
	    #print ex
	    #print "****"
	    if "Primary dataset Name is required for insertPrimaryDataset" in ex.args[0]:
		pass
	    else :
		self.fail("test03: web.DBSWriterModel.insertPrimaryDataset FAILED")

    def test04(self):
	"""test04: web.DBSWriterModel.insertOutputModule: basic test"""
	data = {'release_version': release_version, 'pset_hash': pset_hash, 
	'app_name': app_name, 'output_module_label': output_module_label}
	api.insert('outputconfigs', data)

    def test05(self):
        """test05: web.DBSWriterModel.insertOutputModule: re-insertion should not raise any errors"""
        data = {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name, 'output_module_label': output_module_label}
        api.insert('outputconfigs', data)

	
    def test06(self):
	"""test06: web.DBSWriterModel.insertOutputModule: missing parameter must cause an exception"""
	data = {'pset_hash': pset_hash, 
	#'app_name': app_name, 
	'output_module_label': output_module_label,
	'release_version': release_version}
 	
 	try:
 	    api.insert('outputconfigs', data)
 	except Exception, e:
	    if  'app_name' in e.args[0]:
		pass
	    else:
		self.fail("test06: web.DBSWriterModel.insertOutputModule: missing parameter must cause an exception")
		
    def test07(self):
	"""test07: web.DBSWriterModel.insertAcquisitionEra: Basic test """
	data={'acquisition_era_name': acquisition_era_name}
	api.insert('acquisitioneras', data)

    def test08(self):
	"""test08: web.DBSWriterModel.insertProcessingEra: Basic test """
	data={'processing_version': processing_version, 'description':'this is a test'}
	api.insert('processingeras', data)
	
    	
    def test09(self):
	"""test09: web.DBSWriterModel.insertDataset(Dataset is construct by DBSDatset.): basic test"""
	data = {
		'is_dataset_valid': 1, 'physics_group_name': 'Tracker', 'dataset': dataset,
	        'dataset_access_type': 'PRODUCTION', 'processed_ds_name': procdataset, 'primary_ds_name': primary_ds_name,
		'output_configs': [
		    {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name, 
		    'output_module_label': output_module_label},
		    ],
		'global_tag': u'', 'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': tier,
		'processing_version': processing_version,  'acquisition_era_name': acquisition_era_name,
		}
	#import pdb
	#pdb.set_trace()
	api.insert('datasets', data)
	childdata = {
		'is_dataset_valid': 1, 'physics_group_name': 'Tracker', 'dataset': child_dataset,
	        'dataset_access_type': 'PRODUCTION', 'processed_ds_name': "child_"+procdataset, 'primary_ds_name': primary_ds_name,
		'output_configs': [
		    {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name, 
		    'output_module_label': output_module_label},
		    ],
		'global_tag': u'', 'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': tier,
		'processing_version': processing_version,  'acquisition_era_name': acquisition_era_name,
		}
	api.insert('datasets', childdata)

	
    def test10(self):
	#import pdb
	#pdb.set_trace()
	"""test10: web.DBSWriterModel.insertDataset: duplicate insert should be ignored"""
	data = {
		'is_dataset_valid': 1, 'physics_group_name': 'Tracker', 'dataset': dataset,
	        'dataset_access_type': 'PRODUCTION', 'processed_ds_name': procdataset, 'primary_ds_name': primary_ds_name,
		'output_configs': [
		    {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': 
		    app_name, 'output_module_label': output_module_label},
		], 
		'global_tag': u'', 'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': tier,
		#'processing_version': '1',  'acquisition_era_name': u'',
		}
	
	api.insert('datasets', data)

    def test11(self):
	"""test11: web.DBSWriterModel.insertDataset: missing primary_ds_name must raise an error"""
	data = {
		'is_dataset_valid': 1, 'physics_group_name': 'Tracker', 
		#'primary_ds_name': primary_ds_name,
		'dataset': dataset,
	        'dataset_access_type': 'PRODUCTION', 'processed_ds_name': procdataset,
		'output_configs': [
		    {'release_version': release_version, 'pset_hash': pset_hash, 
		    'app_name': app_name, 'output_module_label': output_module_label},
		],
		'global_tag': u'', 'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': tier,
		}
	try:
	    api.insert('datasets', data)
	except Exception, e:
	    if 'primary_ds_name' in e.args[0]:
		pass
	    else:
		self.fail("test11: web.DBSWriterModel.insertDataset FAILED")
	    
    def test12(self):
	"""test12: web.DBSWriterModel.insertDataset: missing parameter must raise an error"""
	data = {
		'is_dataset_valid': 1, 'physics_group_name': 'Tracker', 'primary_ds_name': primary_ds_name,
	        #'dataset_access_type': 'PRODUCTION', 
		'processed_ds_name': procdataset,
		'output_configs': [
		    {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': 
		    app_name, 'output_module_label': output_module_label},
		],
		'global_tag': u'', 'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': tier,
		'processing_version': processing_version,  'acquisition_era_name': acquisition_era_name,
		}
	try:
	    api.insert('datasets', data)
	except Exception, ex:
	    if 'dataset_access_type' in ex.args[0]:
		pass
	    else:
		self.fail("Exception missing dataset_access_type was expected and was not raised.")
	    
    def test13(self):
	"""test13: web.DBSWriterModel.insertDataset: no output_configs, should be fine insert!"""
	data = {
		'dataset': dataset,
		'is_dataset_valid': 1, 'physics_group_name': 'Tracker', 'primary_ds_name': primary_ds_name,
	        'dataset_access_type': 'PRODUCTION', 'processed_ds_name': procdataset,
		'global_tag': u'', 'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': tier
		}
	api.insert('datasets', data)

    def test14(self):
	"""test14: web.DBSWriterModel.insertSite: basic test"""
	data = {
	     "site_name" : site
	}
	api.insert('sites', data)


    def test15(self):
	"""test15: web.DBSWriterModel.insertSite: duplicate site must not throw any errors"""
	data = {
	     "site_name" : site
	}
        api.insert('sites', data)
	
    def test16(self):
	"""test16 web.DBSWriterModel.insertBlock: basic test"""
	data = {'block_name': block,
		'origin_site_name': site }
		
	api.insert('blocks', data)
	# insert the child block as well
	data = {'block_name': child_block, 'origin_site_name': site }
	api.insert('blocks', data)

    def test17(self):
	"""test17 web.DBSWriterModel.insertBlock: duplicate insert should not raise exception"""
	data = {'block_name': block,
		'origin_site_name': site }
		
	api.insert('blocks', data)

    def test18(self):
	"""test18 web.DBSWriterModel.insertFiles: basic test"""
	data={}
	flist=[]
 	for i in range(10):
	    f={  
		'adler32': u'NOTSET', 'file_type': 'EDM',
                'file_output_config_list': 
		    [ 
			{'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name, 
			'output_module_label': output_module_label},
		    ],
                'dataset': dataset,
                'file_size': u'2012211901', 'auto_cross_section': 0.0, 
                'check_sum': u'1504266448',
                'file_lumi_list': [
	                              {'lumi_section_num': u'27414', 'run_num': uid},
		                      {'lumi_section_num': u'26422', 'run_num': uid},
		                      {'lumi_section_num': u'29838', 'run_num': uid}
                                  ],
                'file_parent_list': [ ],
                'event_count': u'1619',
                'logical_file_name': "/store/mc/%s/%i.root" %(uid,i),
                'block_name': block
			    #'is_file_valid': 1
                }
	    flist.append(f)
	data={"files":flist}
	api.insert('files', data)
	time.sleep(10)

    def test19(self):
	"""test19 web.DBSWriterModel.insertFiles: duplicate insert file shuld not raise any errors"""
	data={}
	flist=[]
 	for i in range(10):
	    f={  
		'adler32': u'NOTSET', 'file_type': 'EDM',
                'file_output_config_list': 
		    [ 
			{'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name, 
			'output_module_label': output_module_label},
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
                'logical_file_name': "/store/mc/%s/%i.root" %(uid,i),
                'block_name': block
			    #'is_file_valid': 1
                }
	    flist.append(f)
	    outDict['files'].append(f['logical_file_name'])
	data={"files":flist}
	api.insert('files', data)

    def test20(self):
	"""test20 web.DBSWriterModel.insertFiles: with parents"""
	#import pdb
	#pdb.set_trace()

        data={}
        flist=[]
        for i in range(10):
            f={
                'adler32': u'NOTSET', 'file_type': 'EDM',
                'file_output_config_list':
                    [
                        {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                        'output_module_label': output_module_label},
                    ],
                'dataset': child_dataset,
                'file_size': u'2012211901', 'auto_cross_section': 0.0,
                'check_sum': u'1504266448',
                'file_lumi_list': [
                                      {'lumi_section_num': u'27414', 'run_num': u'1'},
                                      {'lumi_section_num': u'26422', 'run_num': u'1'},
                                      {'lumi_section_num': u'29838', 'run_num': u'1'}
                                  ],
                'file_parent_list': [{"file_parent_lfn": "/store/mc/%s/%i.root" %(uid, i)}],
                'event_count': u'1619',
                'logical_file_name': "/store/mc/%s-child/%i.root" %(uid, i),
                'block_name': child_block
                            #'is_file_valid': 1
                }
            flist.append(f)
            outDict['files'].append(f['logical_file_name'])
	    outDict['parent_files'].append(f['file_parent_list'][0]['file_parent_lfn'])
        data={"files":flist}
        api.insert('files', data)
	
    def test21(self):
	"""test21 web.DBSWriterModel.updateFileStatus: Basic test """
	lfn = "/store/mc/%s-child/%i.root" %(uid, 1)
	print lfn
	api.update('files', logical_file_name=lfn, is_file_valid=0)


    def test22(self):
        """test22 web.DBSWriterModel.updateDatasetStatus: Basic test """
        api.update('datasets', dataset=dataset, is_dataset_valid=0)
        api.update('datasets', dataset=dataset, is_dataset_valid=1)

    def test23(self):
        """test23 web.DBSWriterModel.updateDatasetType: Basic test """
        api.update('datasets', dataset=dataset, dataset_access_type="DEPRECATED") 

if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSWriterModel_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
    infoout=open("info.dict", "w")
    infoout.write("info="+str(outDict))
    infoout.close()	
