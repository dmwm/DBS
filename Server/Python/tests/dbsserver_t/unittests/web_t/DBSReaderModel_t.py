"""
web unittests
"""

__revision__ = "$Id: DBSReaderModel_t.py,v 1.25 2010/08/30 18:04:20 afaq Exp $"
__version__ = "$Revision: 1.25 $"

import os, sys, imp
import json
import unittest
from dbsserver_t.utils.DBSRestApi import DBSRestApi
from DBSWriterModel_t import outDict

def importCode(code,name,add_to_sys_modules=0):
    module = imp.new_module(name)
    exec code in module.__dict__
    if add_to_sys_modules:
        sys.modules[name] = module
    return module

config = os.environ["DBS_TEST_CONFIG_WRITER"]
service = os.environ["DBS_TEST_SERVICE"]
api = DBSRestApi(config, service)

class DBSReaderModel_t(unittest.TestCase):

    def setUp(self):
        """setup all necessary parameters"""
        global testparams
        if len(testparams) == 0:
            testparams = outDict
	#import pdb
	#pdb.set_trace()
    def test001(self):
        """test001: web.DBSReaderModel.listPrimaryDatasets: basic test"""
        api.list('primarydatasets')

    def test002(self):
	"""test002: web.DBSReaderModel.listPrimaryDatasets: basic test"""
	api.list('primarydatasets', primary_ds_name='*')
    
    def test003(self):
	"""test003: web.DBSReaderModel.listPrimaryDatasets: basic test"""
	api.list('primarydatasets', primary_ds_name=testparams['primary_ds_name'])

    def test004(self):
        """test004: web.DBSReaderModel.listPrimaryDatasets: basic test"""
	api.list('primarydatasets', primary_ds_name=testparams['primary_ds_name']+'*')
       
    def test005(self):
        """test005: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets')

    def test0050(self):
        """test005: web.DBSReaderModel.listDatasets: detail"""
        api.list('datasets', detail=True)
    
    def test006(self):
        """test006: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', dataset='*')

    def test007(self):
        """test007: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', run_num=testparams['run_num'])

    def test008(self):
        """test008: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', dataset=testparams['dataset'])

    def test009(self):
        """test009: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', primary_ds_type=testparams['primary_ds_type'])

    def test010(self):
        """test010: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets',  primary_ds_name=testparams['primary_ds_name'])

    def test011(self):
        """test011: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', dataset=testparams['dataset']+'*')

    def test012(self):
        """test012: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', parent_dataset='*')

    def test013(self):
        """test013: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', data_tier_name='*RAW*')   
 
    def test014(self):
        """test014: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', release_version='*')

    def test015(self):
        """test015: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', physics_group_name='QCD')

    def test016(self):
        """test016: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', release_version=testparams['release_version'])

    def test017(self):
        """test017: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', dataset_access_type='READONLY')

    def test018(self):
        """test018: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', release_version=testparams['release_version']+'*')

    def test019(self):
        """test019: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', logical_file_name=testparams['files'][0])

    def test020(self):
        """test020: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', pset_hash='*')

    def test021(self):
        """test021: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', pset_hash=testparams['pset_hash'])

    def test022(self):
        """test022: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', app_name='*')

    def test023(self):
        """test023: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', app_name=testparams['app_name'])
    
    def test024(self):
        """test024: web.DBSReaderModel.listDatasets: basic test"""
	api.list('datasets', output_module_label='*')

    def test025(self):
        """test025: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', output_module_label=testparams['output_module_label'])

    def test026(self):
        """test026: web.DBSReaderModel.listDatasets: dataset, parent_dataset, release_version, pset_hash, app_name, output_module_label"""
	api.list('datasets', dataset=testparams['dataset'], 
                                  parent_dataset='*',
                                  release_version=testparams['release_version'],
                                  pset_hash=testparams['pset_hash'],
                                  app_name=testparams['app_name'],
                                  output_module_label=testparams['output_module_label'])

    def test0260(self):
        """test0260: web.DBSReaderModel.listDatasets: dataset, parent_dataset, release_version, pset_hash, app_name, output_module_label, detail"""
	api.list('datasets', dataset=testparams['dataset'], 
                                  parent_dataset='*',
                                  release_version=testparams['release_version'],
                                  pset_hash=testparams['pset_hash'],
                                  app_name=testparams['app_name'],
                                  output_module_label=testparams['output_module_label'],
				  detail=True)

    def test027(self):
        """test027: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', dataset=testparams['dataset'],
                                  release_version=testparams['release_version']
                                  )

    def test028(self):
        """test028: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', release_version=testparams['release_version'],
                                  pset_hash=testparams['pset_hash'],
                                  )

    def test029(self):
        """test029: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', app_name=testparams['app_name'],
                             output_module_label=testparams['output_module_label'])

    def test030(self):
        """test030: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', dataset=testparams['dataset'],
                                  app_name=testparams['app_name'],
                                  output_module_label=testparams['output_module_label'])

    def test0300(self):
        """test0300: web.DBSReaderModel.listDatasets: dataset, app_name, output_module_label, detail"""
        api.list('datasets', dataset=testparams['dataset'],
                                  app_name=testparams['app_name'],
                                  output_module_label=testparams['output_module_label'], 
				  detail = True)

    def test031(self):
        """test031: web.DBSReaderModel.listBlocks: basic negative test"""
	try:
	    api.list('blocks', dataset='*')
        except:
	    pass
	else:
	    self.fail("Exception was expected and was not raised.")

    def test032(self):
        """test032: web.DBSReaderModel.listBlocks: basic test"""
	api.list('blocks', dataset=testparams['dataset'])

    def test033(self):
        """test033: web.DBSReaderModel.listBlocks: basic test"""
        api.list('blocks', block_name=testparams['block'])

    def test034(self):
        """test034: web.DBSReaderModel.listBlocks: basic negative test"""
	try:
	    api.list('blocks', origin_site_name=testparams['site'])
	except:
	    pass
	else:
	    self.fail("Exception was expected and was not raised.")
    
    def test035(self):
	"""test035: web.DBSReaderModel.listBlocks: basic test"""
	api.list('blocks', run_num=testparams['run_num'], block_name=testparams['block'])
	

    def test036(self):
        """test036: web.DBSReaderModel.listBlocks: basic negative test"""
        try:
            api.list('blocks', block_name='*')
        except:
            pass
        else:
            self.fail("Exception was expected and was not raised.")

    def test037(self):
        """test037: web.DBSReaderModel.listBlocks: basic negative test"""
        try:
            api.list('blocks', origin_site_name='*')
        except:
            pass
        else:
            self.fail("Exception was expected and was not raised.")
    
    def test038(self):
	 """test038: web.DBSReaderModel.listBlocks: basic test"""
	 api.list('blocks', dataset=testparams['dataset'],
	           origin_site_name=testparams['site'])
		   
    def test039(self):
	"""test039: web.DBSReaderModel.listBlocks: basic test"""
	api.list('blocks', dataset=testparams['dataset'],
		origin_site_name=testparams['site'])

    def test040(self):
        """test040: web.DBSReaderModel.listBlocks: basic test"""
        api.list('blocks', dataset=testparams['dataset'],
                                block_name=testparams['block'],
                                origin_site_name=testparams['site'])
        
    def test041(self):
        """test041: web.DBSReaderModel.listBlocks: Must raise an exception if no parameter is passed."""
	
        try:
	    api.list('blocks')
        except: 
	    pass
        else: 
	    self.fail("Exception was expected and was not raised.")
            
    def test042(self):
        """test042: web.DBSReaderModel.listFiles: basic negative test"""
	try:
	    api.list('files', dataset='*')
	except:
	    pass
	else:
	    self.fail("Exception was expected and was not raised.")
	    
    def test043(self):
	"""test043: web.DBSReaderModel.listFiles: basic test"""
	api.list('files', dataset=testparams['dataset'], minrun=1, maxrun=testparams['run_num'])
    
    def test044(self):
        """test044: web.DBSReaderModel.listFiles: basic test"""
	api.list('files', dataset=testparams['dataset'])

    def test045(self):
        """test045: web.DBSReaderModel.listFiles: with dataset, lumi list"""
        api.list('files', dataset=testparams['dataset'], lumi_list='[27414, 26422, 29838]', maxrun=testparams['run_num'], 
		  minrun=testparams['run_num'])

    def test0450(self):
        """test0450: web.DBSReaderModel.listFiles: with dataset and lumi intervals"""
        api.list('files', dataset=testparams['dataset'], lumi_list='[[1, 100]]', maxrun=testparams['run_num'], 
		  minrun=testparams['run_num'])

    def test046(self):
        """test046: web.DBSReaderModel.listFiles: basic negative test"""
	try:
	    api.list('files', dataset=testparams['dataset']+'*')
	except:
	    pass
	else:
	    self.fail("Exception was expected and was not raised.")

    def test047(self):
        """test047: web.DBSReaderModel.listFiles: basic test"""
        api.list('files', dataset=testparams['dataset'], origin_site_name=testparams['site'])


    def test048(self):
        """test048: web.DBSReaderModel.listFiles: basic negative test"""
	try:
	    api.list('files', block_name='*')
	except:
            pass
        else:
            self.fail("Exception was expected and was not raised.")

    def test049(self):
        """test049: web.DBSReaderModel.listFiles: basic test"""
	api.list('files', block_name=testparams['block'])

    def test050(self):
        """test050: web.DBSReaderModel.listFiles: basic negative test"""
	try:
	    api.list('files', logical_file_name='*')
	except:
            pass
        else:
            self.fail("Exception was expected and was not raised.")

    def test051(self):
        """test051: web.DBSReaderModel.listFiles: basic test"""
	lfn= testparams['files'][1]
	api.list('files', logical_file_name=lfn)	

    def test052(self):
        """test052: web.DBSReaderModel.listFiles: Must raise an exception if no parameter is passed."""
        try: api.list('files')
        except: pass
        else: self.fail("Exception was expected and was not raised")
       
    def test053(self):
        """test053: web.DBSReaderModel.listDatasetParents: basic test"""
        api.list('datasetparents', dataset="*")

    def test054(self):
        """test054: web.DBSReaderModel.listDatasetParents: basic test"""
        api.list('datasetparents', dataset=testparams['dataset'])

    def test055(self):
        """test055: web.DBSReaderModel.listDatasetParents: basic test"""
        api.list('datasetparents', dataset=testparams['dataset']+'*')
        
    def test056(self):
        """test056: web.DBSReaderModel.listDatasetParents: must raise an exception if no parameter is passed"""
        try: 
	    api.list('datasetparents')
        except: 
	    pass
        else: 
	    self.fail("Exception was expected and was not raised")
            
    def test057(self):
        """test057: web.DBSReaderModel.listOutputConfigs: basic test"""
	api.list('outputconfigs')
    
    def test058(self):
        """test058: web.DBSReaderModel.listOutputConfigs: basic test"""
	api.list('outputconfigs', dataset="*")

    def test059(self):
        """test059: web.DBSReaderModel.listOutputConfigs: basic test"""
        api.list('outputconfigs', dataset=testparams['dataset'])

    def test060(self):
        """test060: web.DBSReaderModel.listOutputConfigs: basic test"""
        api.list('outputconfigs', dataset=testparams['dataset']+"*")
	
    def test061(self):
        """test061: web.DBSReaderModel.listOutputConfigs: basic test"""
        api.list('outputconfigs', logical_file_name="*")

    def test062(self):
        """test062: web.DBSReaderModel.listOutputConfigs: basic test"""
	#need to be updated with LFN
	lfn= testparams['files'][1]
        api.list('outputconfigs', logical_file_name=lfn)

    def test063(self):
        """test063: web.DBSReaderModel.listOutputConfigs: basic test"""
        #need to be updated with LFN 
	lfn= testparams['files'][1]
        api.list('outputconfigs', logical_file_name=lfn+"*")

    def test064(self):
        """test064: web.DBSReaderModel.listOutputConfigs: basic test"""
        api.list('outputconfigs', release_version="*")

    def test065(self):
        """test065: web.DBSReaderModel.listOutputConfigs: basic test"""
        api.list('outputconfigs', release_version=testparams['release_version'])

    def test066(self):
        """test066: web.DBSReaderModel.listOutputConfigs: basic test"""
        api.list('outputconfigs', release_version=testparams['release_version']+'*')

    def test067(self):
        """test067: web.DBSReaderModel.listOutputConfigs: basic test"""
        api.list('outputconfigs', pset_hash="*")

    def test068(self):
        """test068: web.DBSReaderModel.listOutputConfigs: basic test"""
        api.list('outputconfigs', pset_hash=testparams['pset_hash'])

    def test069(self):
	"""test069: web.DBSReaderModel.listOutputConfigs: basic test"""
	api.list('outputconfigs', app_name="*")

    def test070(self):
        """test070: web.DBSReaderModel.listOutputConfigs: basic test"""
        api.list('outputconfigs', app_name=testparams['app_name'])

    def test071(self):
        """test071: web.DBSReaderModel.listOutputConfigs: basic test"""
        api.list('outputconfigs', app_name=testparams['app_name']+"*")

    def test072(self):
        """test072: web.DBSReaderModel.listOutputConfigs: basic test"""
	api.list('outputconfigs', output_module_label="*")
 
    def test073(self):
        """test073: web.DBSReaderModel.listOutputConfigs: basic test"""
        api.list('outputconfigs', output_module_label=testparams['output_module_label'])

    def test074(self):
        """test074: web.DBSReaderModel.listOutputConfigs: basic test"""
        api.list('outputconfigs', output_module_label=testparams['output_module_label']+'*')

    def test075(self):
        """test075: web.DBSReaderModel.listOutputConfigs: basic test"""
	api.list('outputconfigs', dataset=testparams['dataset'],
                                  logical_file_name="*",
                                  release_version=testparams['release_version'],
                                  pset_hash=testparams['pset_hash'],
                                  app_name=testparams['app_name'],
                                  output_module_label=testparams['output_module_label'])

    def test076(self):
        """test076: web.DBSReaderModel.listOutputConfigs: basic test"""
        api.list('outputconfigs', dataset=testparams['dataset'],
                                  release_version=testparams['release_version'],
                                  output_module_label=testparams['output_module_label'])

    def test077(self):
        """test077: web.DBSReaderModel.listOutputConfigs: basic test"""
        api.list('outputconfigs', logical_file_name="*",
                                  app_name=testparams['app_name'],
                                  output_module_label=testparams['output_module_label'])
    def test078(self):
        """test078: web.DBSReaderModel.listOutputConfigs: basic test"""
        api.list('outputconfigs', dataset=testparams['dataset'],
                                  release_version=testparams['release_version'])

    def test079(self):
        """test079: web.DBSReaderModel.listFileParents: basic test"""
        api.list('fileparents', logical_file_name="*")

    def test080(self):
        """test080: web.DBSReaderModel.listFileParents: basic test"""
        api.list('fileparents', logical_file_name="ABC")
    
    def test081(self):
        """test081: web.DBSReaderModel.listFileParents: must raise an exception if no parameter is passed"""
        try: api.list('fileparents')
        except: pass
        else: self.fail("Exception was expected and was not raised")
        
    def test082(self):
        """test082: web.DBSReaderModel.listFileLumis: basic test"""
        api.list('filelumis', logical_file_name="*")

    def test083(self):
        """test083: web.DBSReaderModel.listFileLumis: basic test"""
	#need to update LFN
	lfn= testparams['files'][1]
        api.list('filelumis', logical_file_name=lfn)


    def test084(self):
        """test084: web.DBSReaderModel.listFileLumis: basic test"""
        api.list('filelumis', block_name="*")

    def test085(self):
        """test085: web.DBSReaderModel.listFileLumis: basic test"""
        api.list('filelumis', block_name=testparams['block'])

    def test086(self):
        """test086: web.DBSReaderModel.listFileLumis: basic test"""
        api.list('filelumis', block_name=testparams['block']+'*')

    def test087(self):
        """test087: web.DBSReaderModel.listFileLumis: must raise an exception if no parameter is passed"""
        try: api.list('filelumis')
        except: pass
        else: self.fail("Exception was expected and was not raised")
 
    def test088(self):
        """test088: web.DBSReaderModel.listFile with maxrun, minrun: basic """
        api.list('files', maxrun=testparams['run_num'], minrun=testparams['run_num']-1000, dataset=testparams['dataset'])

    def test0880(self):
        """test0880: web.DBSReaderModel.listFile with maxrun, minrun, dataset and detail"""
        api.list('files', maxrun=testparams['run_num'], minrun=testparams['run_num']-1000, dataset=testparams['dataset'], detail = True)

    def test089(self):
        """test089: web.DBSReaderModel.listFile with maxrun, minrun:basic """
        api.list('files', maxrun=testparams['run_num'], minrun=testparams['run_num']-1000, block_name=testparams['block'])

    def test0890(self):
        """test0890: web.DBSReaderModel.listFile with maxrun, minrun, block_name and detail"""
        api.list('files', maxrun=testparams['run_num'], minrun=testparams['run_num']-1000, block_name=testparams['block'], detail = True)

    def test090(self):
        """test090: web.DBSReaderModel.listFile with original site: basic """
        api.list('files', origin_site_name=testparams['site'], dataset=testparams['dataset'])

    def test0900(self):
        """test0900: web.DBSReaderModel.listFile with original site, dataset and detail """
        api.list('files', origin_site_name=testparams['site'], dataset=testparams['dataset'], detail=True)

    def test091(self):
        """test091: web.DBSReaderModel.listFile with original site and dataset"""
        api.list('files', origin_site_name=testparams['site'], block_name=testparams['block'])

    def test0910(self):
        """test0910: web.DBSReaderModel.listFile with original site, block and detail"""
        api.list('files', origin_site_name=testparams['site'], block_name=testparams['block'], detail=True)

    def test092(self):
        """test092: web.DBSReaderModel.listFile with config info and block"""
        api.list('files',  block_name=testparams['block'],
		release_version=testparams['release_version'] , pset_hash=testparams['pset_hash'], 
		app_name=testparams['app_name'], output_module_label=testparams['output_module_label'])

    def test0920(self):
        """test0920: web.DBSReaderModel.listFile with config info, block and detail """
        api.list('files',  block_name=testparams['block'],
		release_version=testparams['release_version'] , pset_hash=testparams['pset_hash'], 
		app_name=testparams['app_name'], output_module_label=testparams['output_module_label'], detail=True)

    def test093(self):
        """test093: web.DBSReaderModel.listFile with config info and dataset"""
        api.list('files',  dataset=testparams['dataset'], 
                release_version=testparams['release_version'] , pset_hash=testparams['pset_hash'], 
                app_name=testparams['app_name'], output_module_label=testparams['output_module_label'])

    def test0930(self):
        """test0930: web.DBSReaderModel.listFile with config info, dataset and detail"""
        api.list('files',  dataset=testparams['dataset'], 
                release_version=testparams['release_version'] , pset_hash=testparams['pset_hash'], 
                app_name=testparams['app_name'], output_module_label=testparams['output_module_label'], detail=True)

    def test094(self):
        """test094: web.DBSReaderModel.listDatasets with processing era: basic """
        api.list('datasets', processing_version=testparams['processing_version'],) 

    def test095(self):
        """test095: web.DBSReaderModel.listDatasets with acquisition era: basic """
        api.list('datasets', acquisition_era=testparams['acquisition_era']) 

    def test096(self):
        """test096: web.DBSReaderModel.listDataTypes : basic """
        api.list('datatypes' )

    def test097(self):
        """test097: web.DBSReaderModel.listDataTypes : basic """
        api.list('datatypes', dataset=testparams['dataset'] )

    def test098(self):
        """test098: web.DBSReaderModel.listDataTypes : basic """
        api.list('datatypes', datatype=testparams['primary_ds_type'] )

    def test099(self):
	"""test099: web.DBSReaderModel.listRuns: basic""" 
	api.list('runs', minrun=0, maxrun=testparams['run_num'])

    def test100(self):
        """test100: web.DBSReaderModel.listRuns: basic"""
        api.list('runs', minrun=0, maxrun=testparams['run_num'], logical_file_name=testparams['files'][0])

    def test101(self):
        """test101: web.DBSReaderModel.listRuns: basic"""
        api.list('runs', block_name=testparams['block'])

    def test102(self):
        """Test102: web.DBSReaderModel.listRuns: basic"""
        api.list('runs', dataset=testparams['dataset'])


 	
    def test103(self):
        """test103:  web.DBSReaderModel.listDatasetParents: basic"""
        api.list('datasetparents', dataset=testparams['dataset'])
        
    def test104(self):
        """test104: web.DBSReaderModel.listDatasetChildren: basic"""
        api.list('datasetchildren', dataset=testparams['dataset'])

    def test105(self):
        """test105: web.DBSReaderModel.listBlockParents: basic"""
        api.list('blockparents', block_name=testparams['block'])

    def test106(self):
        """test106: web.DBSReaderModel.listBlockChildren: basic"""
        api.list('blockchildren', block_name=testparams['block'])
        
    def test107(self):
        """test107: web.DBSReaderModel.listFileParents: basic"""
        api.list('fileparents', logical_file_name=testparams['files'][0])
        
    def test108(self):
        """test108: web.DBSReaderModel.listFileChildren: basic"""
        api.list('filechildren', logical_file_name=testparams['parent_files'][0])
	
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSReaderModel_t)
    infofile=open("info.dict","r")    
    testparams=importCode(infofile, "testparams", 0).info
    unittest.TextTestRunner(verbosity=2).run(SUITE)
else:
    testparams={}
