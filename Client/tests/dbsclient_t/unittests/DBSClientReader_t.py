"""
web unittests
"""

__revision__ = "$Id: DBSClientReader_t.py,v 1.5 2010/01/26 21:02:12 afaq Exp $"
__version__ = "$Revision: 1.5 $"

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
	return
        """unittestDBSClientReader_t.listPrimaryDatasets: basic test"""
        api.listPrimaryDatasets()
        api.listPrimaryDatasets('*')
	api.listPrimaryDatasets(testparams['primary_ds_name'])
	api.listPrimaryDatasets(testparams['primary_ds_name']+"*")

    def test02(self):
	return
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
	return
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
	
	
	#block_name="", dataset="", site_name=""

	
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSClientReader_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
        
