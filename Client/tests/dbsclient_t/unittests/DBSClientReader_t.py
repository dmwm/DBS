"""
web unittests
"""

__revision__ = "$Id: DBSClientReader_t.py,v 1.6 2010/01/26 22:18:29 afaq Exp $"
__version__ = "$Revision: 1.6 $"

import os
import json
import unittest
import sys,imp

from dbs.apis.dbsClient import *

url="http://cmssrv18.fnal.gov:8585/dbs3"
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
        api.listPrimaryDatasets('*')
	api.listPrimaryDatasets(testparams['primary_ds_name'])
	api.listPrimaryDatasets(testparams['primary_ds_name']+"*")

    def test02(self):
	"""unittestDBSClientReader_t.listDatasets: basic test"""
	api.listDatasets()
	api.listDatasets(dataset=testparams['dataset'])
	api.listDatasets(dataset=testparams['dataset']+"*")
	api.listDatasets(release_version=testparams['release_version'])
	api.listDatasets(pset_hash=testparams['pset_hash'])
	api.listDatasets(app_name=testparams['app_name'])
	api.listDatasets(output_module_label=testparams['output_module_label'])
	api.listDatasets(release_version=testparams['release_version'], pset_hash=testparams['pset_hash'], \
		app_name=testparams['app_name'], output_module_label=testparams['output_module_label'])
	api.listDatasets(dataset=testparams['dataset'], release_version=testparams['release_version'], \
		pset_hash=testparams['pset_hash'], app_name=testparams['app_name'], output_module_label=testparams['output_module_label'])
	api.listDatasets(dataset=testparams['dataset'], release_version=testparams['release_version'])

    def test03(self):
	"""unittestDBSClientReader_t.listOutputModules: basic test"""
	api.listOutputConfigs(dataset=testparams['dataset'])
	api.listOutputConfigs(logical_file_name=testparams['files'][0])
	api.listOutputConfigs()
	api.listOutputConfigs(release_version=testparams['release_version'])
	api.listOutputConfigs(pset_hash=testparams['pset_hash'])
	api.listOutputConfigs(app_name=testparams['app_name'])
	api.listOutputConfigs(output_module_label=testparams['output_module_label'])
	api.listOutputConfigs(release_version=testparams['release_version'], pset_hash=testparams['pset_hash'], \
		 app_name=testparams['app_name'], output_module_label=testparams['output_module_label'])
	api.listOutputConfigs(dataset=testparams['dataset'], release_version=testparams['release_version'], \
		pset_hash=testparams['pset_hash'], app_name=testparams['app_name'], output_module_label=testparams['output_module_label'])
	api.listOutputConfigs(dataset=testparams['dataset'], release_version=testparams['release_version'])

    def test04(self):
	"""unittestDBSClientReader_t.listBlocks: basic test"""
	api.listBlocks(block_name=testparams['block'])
	api.listBlocks(dataset=testparams['dataset'])
	api.listBlocks(block_name=testparams['block'], site_name=testparams['site'])
	api.listBlocks(dataset=testparams['dataset'], site_name=testparams['site'])
	api.listBlocks(dataset=testparams['dataset'], block_name=testparams['block'], site_name=testparams['site'])
	try: 
	    api.listBlocks(site_name=testparams['site'])
	except:
	    pass
	else: 
	    self.fail("exception was excepted, was not raised")
	
    def test05(self):
	"""unittestDBSClientReader_t.listDatasetParents basic test"""
	api.listDatasetParents(dataset=testparams['dataset'])	
	api.listDatasetParents(dataset='doesnotexists')

    def test06(self):
	"""unittestDBSClientReader_t.listFiles: basic test"""
	api.listFiles(dataset=testparams['dataset'])
	api.listFiles(block=testparams['block'])
	api.listFiles(lfn=testparams['files'][0])	
	api.listFiles(dataset="doesnotexist")
	api.listFiles(block="doesnotexist#123")
	api.listFiles(lfn="doesnotexist")

    def test07(self):
	"""unittestDBSClientReader_t.listFileParents: basic test"""	
	api.listFileParents(lfn=testparams['files'][0])
	try:
	    api.listFileParents()
	except:
	    pass
	else:
	    self.fail("exception was excepted, was not raised")
	api.listFileParents(lfn="doesnotexist")

if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSClientReader_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
        
