"""
web unittests
"""

__revision__ = "$Id: DBSClientReader_t.py,v 1.19 2010/05/28 21:20:36 afaq Exp $"
__version__ = "$Revision: 1.19 $"

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
        """test01 unittestDBSClientReader_t.listPrimaryDatasets: basic test"""
        api.listPrimaryDatasets()

    def test02(self):
	"""test02 unittestDBSClientReader_t.listPrimaryDatasets: """
        api.listPrimaryDatasets('*')

    def test03(self):
	"""test03 unittestDBSClientReader_t.listPrimaryDatasets: """
	api.listPrimaryDatasets(testparams['primary_ds_name'])

    def test04(self):
	"""test04 unittestDBSClientReader_t.listPrimaryDatasets: """
	api.listPrimaryDatasets(testparams['primary_ds_name']+"*")

    def test05(self):
	"""test05 unittestDBSClientReader_t.listDatasets: basic test"""
	api.listDatasets()
    
    def test06(self):
	"""test06 unittestDBSClientReader_t.listDatasets: """
	api.listDatasets(dataset=testparams['dataset'])
    
    def test07(self):
	"""test07 unittestDBSClientReader_t.listDatasets: """
	api.listDatasets(dataset=testparams['dataset']+"*")
    
    def test08(self):
	"""test08 unittestDBSClientReader_t.listDatasets: """
	api.listDatasets(release_version=testparams['release_version'])
    
    
    def test09(self):
	"""test09 unittestDBSClientReader_t.listDatasets: """
	api.listDatasets(pset_hash=testparams['pset_hash'])
    
    def test10(self):
	"""test10 unittestDBSClientReader_t.listDatasets: """
	api.listDatasets(app_name=testparams['app_name'])
	
    def test11(self):
	"""test11 unittestDBSClientReader_t.listDatasets: """
	api.listDatasets(output_module_label=testparams['output_module_label'])

    def test12(self):
	"""test12 unittestDBSClientReader_t.listDatasets: """
	api.listDatasets(release_version=testparams['release_version'], pset_hash=testparams['pset_hash'], \
		app_name=testparams['app_name'], output_module_label=testparams['output_module_label'])

    def test13(self):
	"""test13 unittestDBSClientReader_t.listDatasets: """
	api.listDatasets(dataset=testparams['dataset'], release_version=testparams['release_version'], \
		pset_hash=testparams['pset_hash'], app_name=testparams['app_name'], output_module_label=testparams['output_module_label'])

    def test14(self):
	"""test14 unittestDBSClientReader_t.listDatasets: """
	api.listDatasets(dataset=testparams['dataset'], release_version=testparams['release_version'])

    def test15(self):
	"""test15 unittestDBSClientReader_t.listOutputModules: basic test"""
	api.listOutputConfigs(dataset=testparams['dataset'])

    def test16(self):
	"""test16 unittestDBSClientReader_t.listOutputModules: """
	api.listOutputConfigs(logical_file_name=testparams['files'][0])

    def test17(self):
	"""test17 unittestDBSClientReader_t.listOutputModules: """
	api.listOutputConfigs()
    
    def test18(self):
	"""test18 unittestDBSClientReader_t.listOutputModules: """
	api.listOutputConfigs(release_version=testparams['release_version'])
    
    def test19(self):
	"""test19 unittestDBSClientReader_t.listOutputModules: """
	api.listOutputConfigs(pset_hash=testparams['pset_hash'])
    
    def test20(self):
	"""test20 unittestDBSClientReader_t.listOutputModules: """
	api.listOutputConfigs(app_name=testparams['app_name'])
    
    def test21(self):
	"""test21 unittestDBSClientReader_t.listOutputModules: """
	api.listOutputConfigs(output_module_label=testparams['output_module_label'])
    
    def test22(self):
	"""test22 unittestDBSClientReader_t.listOutputModules: """
	api.listOutputConfigs(release_version=testparams['release_version'], pset_hash=testparams['pset_hash'], \
		 app_name=testparams['app_name'], output_module_label=testparams['output_module_label'])
    
    def test23(self):
	"""test23 unittestDBSClientReader_t.listOutputModules: """
	api.listOutputConfigs(dataset=testparams['dataset'], release_version=testparams['release_version'], \
		pset_hash=testparams['pset_hash'], app_name=testparams['app_name'], output_module_label=testparams['output_module_label'])
    
    def test24(self):
	"""test24 unittestDBSClientReader_t.listOutputModules: """
	api.listOutputConfigs(dataset=testparams['dataset'], release_version=testparams['release_version'])

    def test25(self):
	"""test25 unittestDBSClientReader_t.listBlocks: basic test"""
	api.listBlocks(block_name=testparams['block'])

    def test26(self):
	"""test26 unittestDBSClientReader_t.listBlocks: """
	api.listBlocks(dataset=testparams['dataset'])
    
    def test27(self):
	"""test27 unittestDBSClientReader_t.listBlocks: """
	api.listBlocks(block_name=testparams['block'], origin_site_name=testparams['site'])

    def test28(self):
	"""test28 unittestDBSClientReader_t.listBlocks: """
	api.listBlocks(dataset=testparams['dataset'], origin_site_name=testparams['site'])
    
    def test29(self):
	"""test29 unittestDBSClientReader_t.listBlocks: """
	api.listBlocks(dataset=testparams['dataset'], block_name=testparams['block'], origin_site_name=testparams['site'])
	try: 
	    api.listBlocks(origin_site_name=testparams['site'])
	except:
	    pass
	else: 
	    self.fail("exception was excepted, was not raised")
	
    def test30(self):
	"""test30 unittestDBSClientReader_t.listDatasetParents basic test"""
	api.listDatasetParents(dataset=testparams['dataset'])	
    
    def test31(self):
	"""test31 unittestDBSClientReader_t.listDatasetParents basic test"""
	api.listDatasetParents(dataset='doesnotexists')

    def test32(self):
	"""test32 unittestDBSClientReader_t.listFiles: basic test"""
	api.listFiles(dataset=testparams['dataset'])
    
    def test33(self):
	"""test33 unittestDBSClientReader_t.listFiles: basic test"""
	api.listFiles(block=testparams['block'])
    
    def test34(self):
	"""test34 unittestDBSClientReader_t.listFiles: basic test"""
	api.listFiles(logical_file_name=testparams['files'][0])	
    
    def test35(self):
	"""test35 unittestDBSClientReader_t.listFiles: basic test"""
	api.listFiles(dataset=testparams['dataset'], release_version=testparams['release_version'])
    
    def test36(self):
	"""test36 unittestDBSClientReader_t.listFiles: basic test"""
	api.listFiles(dataset=testparams['dataset'], release_version=testparams['release_version'], \
		                pset_hash=testparams['pset_hash'], app_name=testparams['app_name'], output_module_label=testparams['output_module_label'])
    def test37(self):
	"""test37 unittestDBSClientReader_t.listFiles: basic test"""
	api.listFiles(logical_file_name=testparams['files'][0], pset_hash=testparams['pset_hash'], app_name=testparams['app_name'], output_module_label=testparams['output_module_label'])
    
    def test38(self):
	"""test38 unittestDBSClientReader_t.listFiles: basic test"""
	api.listFiles(dataset="doesnotexist")
    
    def test39(self):
	"""test39 unittestDBSClientReader_t.listFiles: basic test"""
	api.listFiles(block="doesnotexist#123")
    
    def test40(self):
	"""test40 unittestDBSClientReader_t.listFiles: basic test"""
	api.listFiles(logical_file_name="doesnotexist")

    def test41(self):
	"""test41 unittestDBSClientReader_t.listFileParents: basic test"""	
	api.listFileParents(logical_file_name=testparams['files'][0])
    
    def test42(self):
	"""test42 unittestDBSClientReader_t.listFileParents: basic test"""	
	try:
	    print api.listFileParents()
	except:
	    pass
	else:
	    self.fail("exception was excepted, was not raised")
    
    def test43(self):
	"""test43 unittestDBSClientReader_t.listFileParents: basic test"""	
	api.listFileParents(logical_file_name="doesnotexist")

    def test44(self):
	"""test44 unittestDBSClientReader_t.listFileLumis: basic test"""	
	api.listFileLumis(logical_file_name=testparams['files'][0])
    
    def test45(self):
	"""test45 unittestDBSClientReader_t.listFileLumis: basic test"""	
	try:
	    print api.listFileLumis()
	except:
	    pass
	else:
	    self.fail("exception was excepted, was not raised")
    
    def test46(self):
	"""test46 unittestDBSClientReader_t.listFileLumis: basic test"""	
	api.listFileLumis(logical_file_name="doesnotexist")

    """
    def test47(self):
	""test47 unittestDBSClientReader_t.listRuns : basic test""
	api.listRuns()
    
    def test48(self):
	""test48 unittestDBSClientReader_t.listRuns : basic test""
	api.listRuns(dataset=testparams['dataset'])

    def test49(self):
	""test49 unittestDBSClientReader_t.listRuns : basic test""
	api.listRuns(block=testparams['block'])
	
    def test50(self):
	""test50 unittestDBSClientReader_t.listRuns : basic test""
	api.listRuns(logical_file_name=testparams['files'][0])
	
    def test51(self):
	""test51 unittestDBSClientReader_t.listRuns : basic test""
	api.listRuns(dataset=testparams['dataset'], block=testparams['block'], logical_file_name=testparams['files'][0])

    def test52(self):
	""test52 unittestDBSClientReader_t.listRuns : basic test""
	api.listRuns(block=testparams['block'], logical_file_name=testparams['files'][0])

    def test53(self):
	""test53 unittestDBSClientReader_t.listRuns :""
	api.listRuns(dataset=testparams['dataset'], logical_file_name=testparams['files'][0])

    def test54(self):
        ""test54 unittestDBSClientReader_t.listRuns : basic test""
        api.listRuns(minrun=testparams['runs'][1])
 
    def test55(self):
        ""test55 unittestDBSClientReader_t.listRuns : basic test""
        api.listRuns(maxrun=testparams['runs'][2])
    
    def test55(self):
        ""test55 unittestDBSClientReader_t.listRuns : basic test""
        api.listRuns(minrun=testparams['runs'][0], maxrun=testparams['runs'][2])

    def test56(self):
	""test56 unittestDBSClientReader_t.listRuns : basic test""
	api.listRuns(dataset=testparams['dataset'], minrun=testparams['runs'][0], maxrun=testparams['runs'][2])

    def test57(self):
	""test57 unittestDBSClientReader_t.listRuns : basic test""
	api.listRuns(block=testparams['block'], minrun=testparams['runs'][0], maxrun=testparams['runs'][2])
	
    def test58(self):
	""test58 unittestDBSClientReader_t.listRuns : basic test""
	api.listRuns(logical_file_name=testparams['files'][0], minrun=testparams['runs'][0], maxrun=testparams['runs'][2])

    def test59(self):
	""test59 unittestDBSClientReader_t.listRuns : basic test""
	api.listRuns(dataset=testparams['dataset'], block=testparams['block'], logical_file_name=testparams['files'][0], minrun=testparams['runs'][0], maxrun=testparams['runs'][2])

    """


    def test60(self):
	"""test60 unittestDBSClientReader_t.listDatasetParents basic test"""
	api.listDatasetParents(dataset='doesnotexists')

    def test61(self):
	"""test61 unittestDBSClientReader_t.listFiles: basic test"""
	api.listFiles(dataset=testparams['dataset'], minrun=testparams['runs'][0], maxrun=testparams['runs'][2])
    
    def test62(self):
	"""test62 unittestDBSClientReader_t.listFiles: basic test"""
	api.listFiles(block=testparams['block'], minrun=testparams['runs'][0], maxrun=testparams['runs'][2])
    
    def test63(self):
	"""test63 unittestDBSClientReader_t.listFiles: NOT YET SUPPORTED"""
	#api.listFiles(logical_file_name=testparams['files'][0], minrun=testparams['runs'][0], maxrun=testparams['runs'][2])	
	pass
	
    def test64(self):
	"""test64 unittestDBSClientReader_t.listDatasets: processing_version"""
	api.listDatasets(dataset=testparams['dataset'], processing_version=testparams['processing_version'] )
	
    def test65(self):
	"""test65 unittestDBSClientReader_t.listDatasets: acquisition_era"""
	api.listDatasets(dataset=testparams['dataset'], acquisition_era=testparams['acquisition_era'] )

    def test66(self):
	"""test66 unittestDBSClientReader_t.listDatasets: acquisition_era and processing_version both"""
	api.listDatasets(dataset=testparams['dataset'], acquisition_era=testparams['acquisition_era'], processing_version=testparams['processing_version'] )

    def test67(self):
	"""test67 unittestDBSClientReader_t.listDataTypes: basic test"""
	api.listDataTypes()
    
    def test68(self):
	"""test68 unittestDBSClientReader_t.listDataTypes: for a dataset"""
	api.listDataTypes(dataset=testparams['dataset'])
	
    def test69(self):
	"""test74 web.DBSReaderModel.listFile with original site: basic"""
	api.listFiles(origin_site_name=testparams['site'], dataset=testparams['dataset'])

    def test70(self):
	"""test75 web.DBSReaderModel.listFile with original site: basic"""
	api.listFiles(origin_site_name=testparams['site'], block=testparams['block'])

    def test71(self):
	"""list dataset parents"""
	api.listDatasetParents(dataset=testparams['dataset'])
	
    def test72(self):
	"""list dataset children"""
	api.listDatasetChildren(dataset=testparams['parent_dataset'])

    def test73(self):
	"""list block parents"""
	api.listBlockParents(block_name=testparams['block'])

    def test74(self):
	"""list block children"""
	api.listBlockChildren(block_name=testparams['parent_block'])
	
    def test75(self):
	"""list file parents"""
	api.listFileParents(logical_file_name=testparams['files'][0])
	
    def test76(self):
	"""list file children"""
	api.listFileChildren(logical_file_name=testparams['parent_files'][0])

if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSClientReader_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
        
