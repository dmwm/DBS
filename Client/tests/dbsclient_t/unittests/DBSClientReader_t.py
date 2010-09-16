"""
web unittests
"""

__revision__ = "$Id: DBSClientReader_t.py,v 1.10 2010/03/17 21:32:24 afaq Exp $"
__version__ = "$Revision: 1.10 $"

import os
import json
import unittest
import sys,imp

from dbs.apis.dbsClient import *

url=os.environ['DBS_READER_URL'] 
#url="http://cmssrv18.fnal.gov:8585/dbs3"
api = DbsApi(url=url)

def importCode(code,name,add_to_sys_modules=0):
    module = imp.new_module(name)
    exec code in module.__dict__
    if add_to_sys_modules:
        sys.modules[name] = module
    return module

infofile=open("info.dict","r")    
testparams=importCode(infofile, "testparams", 0).info
#['release_version', 'primary_ds_name', 'app_name', 'output_module_label', 'tier', 'pset_hash', 'procdataset', 'site', 'block', 'dataset']    

class DBSClientReader_t(unittest.TestCase):
    
    def test01(self):
        """unittestDBSClientReader_t.listPrimaryDatasets: basic test"""
        api.listPrimaryDatasets()

    def test02(self):
	"""unittestDBSClientReader_t.listPrimaryDatasets: """
        api.listPrimaryDatasets('*')

    def test03(self):
	"""unittestDBSClientReader_t.listPrimaryDatasets: """
	api.listPrimaryDatasets(testparams['primary_ds_name'])

    def test04(self):
	"""unittestDBSClientReader_t.listPrimaryDatasets: """
	api.listPrimaryDatasets(testparams['primary_ds_name']+"*")

    def test05(self):
	"""unittestDBSClientReader_t.listDatasets: basic test"""
	api.listDatasets()
    
    def test06(self):
	"""unittestDBSClientReader_t.listPrimaryDatasets: """
	api.listDatasets(dataset=testparams['dataset'])
    
    def test07(self):
	"""unittestDBSClientReader_t.listPrimaryDatasets: """
	api.listDatasets(dataset=testparams['dataset']+"*")
    
    def test08(self):
	"""unittestDBSClientReader_t.listPrimaryDatasets: """
	api.listDatasets(release_version=testparams['release_version'])
    
    
    def test09(self):
	"""unittestDBSClientReader_t.listPrimaryDatasets: """
	api.listDatasets(pset_hash=testparams['pset_hash'])
    
    def test10(self):
	"""unittestDBSClientReader_t.listPrimaryDatasets: """
	api.listDatasets(app_name=testparams['app_name'])
	
    def test11(self):
	"""unittestDBSClientReader_t.listPrimaryDatasets: """
	api.listDatasets(output_module_label=testparams['output_module_label'])

    def test12(self):
	"""unittestDBSClientReader_t.listPrimaryDatasets: """
	api.listDatasets(release_version=testparams['release_version'], pset_hash=testparams['pset_hash'], \
		app_name=testparams['app_name'], output_module_label=testparams['output_module_label'])

    def test13(self):
	"""unittestDBSClientReader_t.listPrimaryDatasets: """
	api.listDatasets(dataset=testparams['dataset'], release_version=testparams['release_version'], \
		pset_hash=testparams['pset_hash'], app_name=testparams['app_name'], output_module_label=testparams['output_module_label'])

    def test14(self):
	"""unittestDBSClientReader_t.listPrimaryDatasets: """
	api.listDatasets(dataset=testparams['dataset'], release_version=testparams['release_version'])

    def test15(self):
	"""unittestDBSClientReader_t.listOutputModules: basic test"""
	api.listOutputConfigs(dataset=testparams['dataset'])

    def test16(self):
	"""unittestDBSClientReader_t.listOutputModules: """
	api.listOutputConfigs(logical_file_name=testparams['files'][0])

    def test17(self):
	"""unittestDBSClientReader_t.listOutputModules: """
	api.listOutputConfigs()
    
    def test18(self):
	"""unittestDBSClientReader_t.listOutputModules: """
	api.listOutputConfigs(release_version=testparams['release_version'])
    
    def test19(self):
	"""unittestDBSClientReader_t.listOutputModules: """
	api.listOutputConfigs(pset_hash=testparams['pset_hash'])
    
    def test20(self):
	"""unittestDBSClientReader_t.listOutputModules: """
	api.listOutputConfigs(app_name=testparams['app_name'])
    
    def test21(self):
	"""unittestDBSClientReader_t.listOutputModules: """
	api.listOutputConfigs(output_module_label=testparams['output_module_label'])
    
    def test22(self):
	"""unittestDBSClientReader_t.listOutputModules: """
	api.listOutputConfigs(release_version=testparams['release_version'], pset_hash=testparams['pset_hash'], \
		 app_name=testparams['app_name'], output_module_label=testparams['output_module_label'])
    
    def test23(self):
	"""unittestDBSClientReader_t.listOutputModules: """
	api.listOutputConfigs(dataset=testparams['dataset'], release_version=testparams['release_version'], \
		pset_hash=testparams['pset_hash'], app_name=testparams['app_name'], output_module_label=testparams['output_module_label'])
    
    def test24(self):
	"""unittestDBSClientReader_t.listOutputModules: """
	api.listOutputConfigs(dataset=testparams['dataset'], release_version=testparams['release_version'])

    def test25(self):
	"""unittestDBSClientReader_t.listBlocks: basic test"""
	api.listBlocks(block_name=testparams['block'])

    def test26(self):
	"""unittestDBSClientReader_t.listBlocks: """
	api.listBlocks(dataset=testparams['dataset'])
    
    def test27(self):
	"""unittestDBSClientReader_t.listBlocks: """
	api.listBlocks(block_name=testparams['block'], site_name=testparams['site'])

    def test28(self):
	"""unittestDBSClientReader_t.listBlocks: """
	api.listBlocks(dataset=testparams['dataset'], site_name=testparams['site'])
    
    def test29(self):
	"""unittestDBSClientReader_t.listBlocks: """
	api.listBlocks(dataset=testparams['dataset'], block_name=testparams['block'], site_name=testparams['site'])
	try: 
	    api.listBlocks(site_name=testparams['site'])
	except:
	    pass
	else: 
	    self.fail("exception was excepted, was not raised")
	
    def test30(self):
	"""unittestDBSClientReader_t.listDatasetParents basic test"""
	api.listDatasetParents(dataset=testparams['dataset'])	
    
    def test31(self):
	"""unittestDBSClientReader_t.listDatasetParents basic test"""
	api.listDatasetParents(dataset='doesnotexists')

    def test32(self):
	"""unittestDBSClientReader_t.listFiles: basic test"""
	api.listFiles(dataset=testparams['dataset'])
    
    def test33(self):
	"""unittestDBSClientReader_t.listFiles: basic test"""
	api.listFiles(block=testparams['block'])
    
    def test34(self):
	"""unittestDBSClientReader_t.listFiles: basic test"""
	api.listFiles(lfn=testparams['files'][0])	
    
    def test35(self):
	"""unittestDBSClientReader_t.listFiles: basic test"""
	api.listFiles(dataset=testparams['dataset'], release_version=testparams['release_version'])
    
    def test36(self):
	"""unittestDBSClientReader_t.listFiles: basic test"""
	api.listFiles(dataset=testparams['dataset'], release_version=testparams['release_version'], \
		                pset_hash=testparams['pset_hash'], app_name=testparams['app_name'], output_module_label=testparams['output_module_label'])
    def test37(self):
	"""unittestDBSClientReader_t.listFiles: basic test"""
	api.listFiles(lfn=testparams['files'][0], pset_hash=testparams['pset_hash'], app_name=testparams['app_name'], output_module_label=testparams['output_module_label'])
    
    def test38(self):
	"""unittestDBSClientReader_t.listFiles: basic test"""
	api.listFiles(dataset="doesnotexist")
    
    def test39(self):
	"""unittestDBSClientReader_t.listFiles: basic test"""
	api.listFiles(block="doesnotexist#123")
    
    def test40(self):
	"""unittestDBSClientReader_t.listFiles: basic test"""
	api.listFiles(lfn="doesnotexist")

    def test41(self):
	"""unittestDBSClientReader_t.listFileParents: basic test"""	
	api.listFileParents(lfn=testparams['files'][0])
    
    def test42(self):
	"""unittestDBSClientReader_t.listFileParents: basic test"""	
	try:
	    api.listFileParents()
	except:
	    pass
	else:
	    self.fail("exception was excepted, was not raised")
    
    def test43(self):
	"""unittestDBSClientReader_t.listFileParents: basic test"""	
	api.listFileParents(lfn="doesnotexist")

    def test44(self):
	"""unittestDBSClientReader_t.listFileLumis: basic test"""	
	api.listFileLumis(lfn=testparams['files'][0])
    
    def test45(self):
	"""unittestDBSClientReader_t.listFileLumis: basic test"""	
	try:
	    api.listFileLumis()
	except:
	    pass
	else:
	    self.fail("exception was excepted, was not raised")
    
    def test46(self):
	"""unittestDBSClientReader_t.listFileLumis: basic test"""	
	api.listFileLumis(lfn="doesnotexist")

if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSClientReader_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
        
