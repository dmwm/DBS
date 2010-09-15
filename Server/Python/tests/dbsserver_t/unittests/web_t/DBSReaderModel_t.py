"""
web unittests
"""

__revision__ = "$Id: DBSReaderModel_t.py,v 1.12 2010/01/28 15:54:23 afaq Exp $"
__version__ = "$Revision: 1.12 $"

import os, sys, imp
import json
import unittest
from dbsserver_t.utils.DBSRestApi import DBSRestApi

def importCode(code,name,add_to_sys_modules=0):
    module = imp.new_module(name)
    exec code in module.__dict__
    if add_to_sys_modules:
        sys.modules[name] = module
    return module

infofile=open("info.dict","r")    
testparams=importCode(infofile, "testparams", 0).info
config = os.environ["DBS_TEST_CONFIG_READER"]
api = DBSRestApi(config)

print testparams

class DBSReaderModel_t(unittest.TestCase):
    
    	
    def setUp(self):
        """setup all necessary parameters"""
	#import pdb
	#pdb.set_trace()

    def test01(self):
        """Test01 web.DBSReaderModel.listPrimaryDatasets: basic test"""
        api.list('primarydatasets')

    def test02(self):
	"""Test02 web.DBSReaderModel.listPrimaryDatasets: basic test"""
	api.list('primarydatasets', primary_ds_name='*')
       
    def test03(self):
	"""Test03 web.DBSReaderModel.listPrimaryDatasets: basic test"""
	api.list('primarydatasets', primary_ds_name=testparams['primary_ds_name'])

    def test04(self):
        """Test04 web.DBSReaderModel.listPrimaryDatasets: basic test"""
	api.list('primarydatasets', primary_ds_name=testparams['primary_ds_name']+'*')
       
    def test05(self):
        """Test05 web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets')
    
    def test06(self):
        """Test06 web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', dataset='*')

    def test07(self):
        """Test07 web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', dataset=testparams['dataset'])

    def test08(self):
        """Test08 web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', dataset=testparams['dataset']+'*')

    def test09(self):
        """Test09 web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', parent_dataset='*')
    
    def test10(self):
        """Test10 web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', release_version='*')

    def test11(self):
        """Test11 web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', release_version=testparams['release_version'])

    def test12(self):
        """Test12 web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', release_version=testparams['release_version']+'*')

    def test13(self):
        """Test13 web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', pset_hash='*')

    def test14(self):
        """Test14 web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', pset_hash=testparams['pset_hash'])

    def test15(self):
        """Test15 web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', app_name='*')

    def test16(self):
        """Test16 web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', app_name=testparams['app_name'])
    
    def test17(self):
        """Test17 web.DBSReaderModel.listDatasets: basic test"""
	api.list('datasets', output_module_label='*')

    def test18(self):
        """Test18 web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', output_module_label=testparams['output_module_label'])

    def test19(self):
        """Test19 web.DBSReaderModel.listDatasets: basic test"""
	api.list('datasets', dataset=testparams['dataset'], 
                                  parent_dataset='*',
                                  release_version=testparams['release_version'],
                                  pset_hash=testparams['pset_hash'],
                                  app_name=testparams['app_name'],
                                  output_module_label=testparams['output_module_label'])

    def test20(self):
        """Test20 web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', dataset=testparams['dataset'],
                                  release_version=testparams['release_version']
                                  )

    def test21(self):
        """Test21 web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', release_version=testparams['release_version'],
                                  pset_hash=testparams['pset_hash'],
                                  )

    def test22(self):
        """Test22 web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', app_name=testparams['app_name'],
                             output_module_label=testparams['output_module_label'])

    def test23(self):
        """Test23 web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', dataset=testparams['dataset'],
                                  app_name=testparams['app_name'],
                                  output_module_label=testparams['output_module_label'])
    def test24(self):
        """Test24 web.DBSReaderModel.listBlocks: basic test"""
	try:
	    api.list('blocks', dataset='*')
        except:
	    pass
	else:
	    self.fail("Exception was expected and was not raised.")

    def test25(self):
        """Test25 web.DBSReaderModel.listBlocks: basic test"""
	api.list('blocks', dataset=testparams['dataset'])

    def test26(self):
        """Test26 web.DBSReaderModel.listBlocks: basic test"""
        api.list('blocks', block_name=testparams['block'])

    def test27(self):
        """Test27 web.DBSReaderModel.listBlocks: basic test"""
	try:
	    api.list('blocks', site_name=testparams['site'])
	except:
	    pass
	else:
	    self.fail("Exception was expected and was not raised.")

    def test28(self):
        """Test28 web.DBSReaderModel.listBlocks: basic test"""
        try:
            api.list('blocks', block_name='*')
        except:
            pass
        else:
            self.fail("Exception was expected and was not raised.")

    def test29(self):
        """Test29 web.DBSReaderModel.listBlocks: basic test"""
        try:
            api.list('blocks', site_name='*')
        except:
            pass
        else:
            self.fail("Exception was expected and was not raised.")

    def test30(self):
        """Test30 web.DBSReaderModel.listBlocks: basic test"""
        api.list('blocks', dataset=testparams['dataset'],
                                block_name=testparams['block'],
                                site_name=testparams['site'])
        
    def test31(self):
        """Test31 web.DBSReaderModel.listBlocks: Must raise an exception if no parameter is passed."""
	
        try:
	    api.list('blocks')
        except: 
	    pass
        else: 
	    self.fail("Exception was expected and was not raised.")
            
    def test32(self):
        """Test32 web.DBSReaderModel.listFiles: basic test"""
	try:
	    api.list('files', dataset='*')
	except:
	    pass
	else:
	    self.fail("Exception was expected and was not raised.")

    def test33(self):
        """Test33 web.DBSReaderModel.listFiles: basic test"""
	api.list('files', dataset=testparams['dataset'])

    def test34(self):
        """Test34 web.DBSReaderModel.listFiles: basic test"""
	try:
	    api.list('files', dataset=testparams['dataset']+'*')
	except:
	    pass
	else:
	    self.fail("Exception was expected and was not raised.")

    def test35(self):
        """Test35 web.DBSReaderModel.listFiles: basic test"""
	try:
	    api.list('files', block_name='*')
	except:
            pass
        else:
            self.fail("Exception was expected and was not raised.")

    def test35(self):
        """Test35 web.DBSReaderModel.listFiles: basic test"""
	api.list('files', block_name=testparams['block'])

    def test36(self):
        """Test36 web.DBSReaderModel.listFiles: basic test"""
	try:
	    api.list('files', logical_file_name='*')
	except:
            pass
        else:
            self.fail("Exception was expected and was not raised.")

    def test36(self):
        """Test36 web.DBSReaderModel.listFiles: basic test"""
	#need to be updated
	print testparams['files']
	lfn= testparams['files'][1]
	api.list('files', logical_file_name=lfn)	

    def test37(self):
        """Test37 web.DBSReaderModel.listFiles: Must raise an exception if no parameter is passed."""
        try: api.list('files')
        except: pass
        else: self.fail("Exception was expected and was not raised")
            
    def test38(self):
        """Test38 web.DBSReaderModel.listDatasetParents: basic test"""
        api.list('datasetparents', dataset="*")

    def test39(self):
        """Test39 web.DBSReaderModel.listDatasetParents: basic test"""
        api.list('datasetparents', dataset=testparams['dataset'])

    def test40(self):
        """Test40 web.DBSReaderModel.listDatasetParents: basic test"""
        api.list('datasetparents', dataset=testparams['dataset']+'*')
        
    def test41(self):
        """Test41 web.DBSReaderModel.listDatasetParents: must raise an exception if no parameter is passed"""
        try: 
	    api.list('datasetparents')
        except: 
	    pass
        else: 
	    self.fail("Exception was expected and was not raised")
            
    def test42(self):
        """Test42 web.DBSReaderModel.listOutputConfigs: basic test"""
	api.list('outputconfigs')
    
    def test43(self):
        """Test43 web.DBSReaderModel.listOutputConfigs: basic test"""
	api.list('outputconfigs', dataset="*")

    def test44(self):
        """Test44 web.DBSReaderModel.listOutputConfigs: basic test"""
        api.list('outputconfigs', dataset=testparams['dataset'])

    def test45(self):
        """Test45 web.DBSReaderModel.listOutputConfigs: basic test"""
        api.list('outputconfigs', dataset=testparams['dataset']+"*")
	
    def test46(self):
        """Test46 web.DBSReaderModel.listOutputConfigs: basic test"""
        api.list('outputconfigs', logical_file_name="*")

    def test47(self):
        """Test47 web.DBSReaderModel.listOutputConfigs: basic test"""
	#need to be updated with LFN
	lfn= testparams['files'][1]
        api.list('outputconfigs', logical_file_name=lfn)

    def test48(self):
        """Test48 web.DBSReaderModel.listOutputConfigs: basic test""" 
        #need to be updated with LFN 
	lfn= testparams['files'][1]
        api.list('outputconfigs', logical_file_name=lfn+"*")

    def test49(self):
        """Test49 web.DBSReaderModel.listOutputConfigs: basic test"""
        api.list('outputconfigs', release_version="*")

    def test50(self):
        """Test50 web.DBSReaderModel.listOutputConfigs: basic test"""
        api.list('outputconfigs', release_version=testparams['release_version'])

    def test51(self):
        """Test51 web.DBSReaderModel.listOutputConfigs: basic test"""
        api.list('outputconfigs', release_version=testparams['release_version']+'*')

    def test52(self):
        """Test52 web.DBSReaderModel.listOutputConfigs: basic test"""
        api.list('outputconfigs', pset_hash="*")

    def test53(self):
        """Test53 web.DBSReaderModel.listOutputConfigs: basic test"""
        api.list('outputconfigs', pset_hash=testparams['pset_hash'])

    def test54(self):
	"""Test54 web.DBSReaderModel.listOutputConfigs: basic test"""
	api.list('outputconfigs', app_name="*")

    def test55(self):
        """Test55 web.DBSReaderModel.listOutputConfigs: basic test"""
        api.list('outputconfigs', app_name=testparams['app_name'])

    def test56(self):
        """Test56 web.DBSReaderModel.listOutputConfigs: basic test"""
        api.list('outputconfigs', app_name=testparams['app_name']+"*")

    def test56(self):
        """Test56 web.DBSReaderModel.listOutputConfigs: basic test"""
	api.list('outputconfigs', output_module_label="*")
 
    def test57(self):
        """Test57 web.DBSReaderModel.listOutputConfigs: basic test"""
        api.list('outputconfigs', output_module_label=testparams['output_module_label'])

    def test58(self):
        """Test58 web.DBSReaderModel.listOutputConfigs: basic test"""
        api.list('outputconfigs', output_module_label=testparams['output_module_label']+'*')

    def test60(self):
        """Test60 web.DBSReaderModel.listOutputConfigs: basic test"""
	api.list('outputconfigs', dataset=testparams['dataset'],
                                  logical_file_name="*",
                                  release_version=testparams['release_version'],
                                  pset_hash=testparams['pset_hash'],
                                  app_name=testparams['app_name'],
                                  output_module_label=testparams['output_module_label'])

    def test61(self):
        """Test61 web.DBSReaderModel.listOutputConfigs: basic test"""
        api.list('outputconfigs', dataset=testparams['dataset'],
                                  release_version=testparams['release_version'],
                                  output_module_label=testparams['output_module_label'])

    def test62(self):
        """Test62 web.DBSReaderModel.listOutputConfigs: basic test"""
        api.list('outputconfigs', logical_file_name="*",
                                  app_name=testparams['app_name'],
                                  output_module_label=testparams['output_module_label'])
    def test63(self):
        """Test63 web.DBSReaderModel.listOutputConfigs: basic test"""
        api.list('outputconfigs', dataset=testparams['dataset'],
                                  release_version=testparams['release_version'])

    def test64(self):
        """Test64 web.DBSReaderModel.listFileParents: basic test"""
        api.list('fileparents', logical_file_name="*")

    def test65(self):
        """Test65 web.DBSReaderModel.listFileParents: basic test"""
        api.list('fileparents', logical_file_name="ABC")
    
    def test66(self):
        """Test66 web.DBSReaderModel.listFileParents: must raise an exception if no parameter is passed"""
        try: api.list('fileparents')
        except: pass
        else: self.fail("Exception was expected and was not raised")
        
    def test66(self):
        """Test66 web.DBSReaderModel.listFileLumis: basic test"""
        api.list('filelumis', logical_file_name="*")

    def test67(self):
        """Test67 web.DBSReaderModel.listFileLumis: basic test"""
	#need to update LFN
	lfn= testparams['files'][1]
        api.list('filelumis', logical_file_name=lfn)


    def test68(self):
        """Test68 web.DBSReaderModel.listFileLumis: basic test"""
        api.list('filelumis', block_name="*")

    def test69(self):
        """Test69 web.DBSReaderModel.listFileLumis: basic test"""
        api.list('filelumis', block_name=testparams['block'])

    def test70(self):
        """Test70 web.DBSReaderModel.listFileLumis: basic test"""
        api.list('filelumis', block_name=testparams['block']+'*')

    def test71(self):
        """Test71 web.DBSReaderModel.listFileLumis: must raise an exception if no parameter is passed"""
        try: api.list('filelumis')
        except: pass
        else: self.fail("Exception was expected and was not raised")
        
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSReaderModel_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
        
