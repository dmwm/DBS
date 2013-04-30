"""
web unittests
"""
import os
import json
import unittest
import sys,imp

from dbs.apis.dbsClient import *

def importCode(code,name,add_to_sys_modules=0):
    module = imp.new_module(name)
    exec code in module.__dict__
    if add_to_sys_modules:
        sys.modules[name] = module
    return module

class DBSClientReader_t(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super(DBSClientReader_t, self).__init__(methodName)
        url=os.environ['DBS_WRITER_URL']
        proxy=os.environ.get('SOCKS5_PROXY')
        self.api = DbsApi(url=url, proxy=proxy)


    def setUp(self):
        """setup all necessary parameters"""
        infofile=open(os.path.join(os.path.dirname(os.path.abspath(__file__)),"info.dict"),"r")
        self.testparams=importCode(infofile, "testparams", 0).info

    def test00a(self):
        """test00 unittestDBSClientReader_t.requestTimingInfo"""
        self.api.requestTimingInfo

    def test00b(self):
        """test00b unittestDBSClientReader_t.requestContentLength"""
        self.api.requestContentLength

    def test01(self):
        """test01 unittestDBSClientReader_t.listPrimaryDatasets: basic test"""
        self.api.listPrimaryDatasets()

    def test02(self):
	"""test02 unittestDBSClientReader_t.listPrimaryDatasets: """
        self.api.listPrimaryDatasets(primary_ds_name='*')

    def test03(self):
	"""test03 unittestDBSClientReader_t.listPrimaryDatasets: """
	self.api.listPrimaryDatasets(primary_ds_name=self.testparams['primary_ds_name'])

    def test04(self):
	"""test04 unittestDBSClientReader_t.listPrimaryDatasets: """
	self.api.listPrimaryDatasets(primary_ds_name=self.testparams['primary_ds_name']+"*")

    def test05(self):
	"""test05 unittestDBSClientReader_t.listDatasets: basic test"""
	self.api.listDatasets()

    def test06(self):
	"""test06 unittestDBSClientReader_t.listDatasets: """
	self.api.listDatasets(dataset=self.testparams['dataset'])

    def test07(self):
	"""test07 unittestDBSClientReader_t.listDatasets: """
	self.api.listDatasets(dataset=self.testparams['dataset']+"*")

    def test08(self):
	"""test08 unittestDBSClientReader_t.listDatasets: """
	self.api.listDatasets(release_version=self.testparams['release_version'])

    def test09(self):
	"""test09 unittestDBSClientReader_t.listDatasets: """
	self.api.listDatasets(pset_hash=self.testparams['pset_hash'])

    def test10(self):
	"""test10 unittestDBSClientReader_t.listDatasets: """
	self.api.listDatasets(app_name=self.testparams['app_name'])

    def test11(self):
	"""test11 unittestDBSClientReader_t.listDatasets: """
	self.api.listDatasets(output_module_label=self.testparams['output_module_label'])

    def test12(self):
	"""test12 unittestDBSClientReader_t.listDatasets: """
	self.api.listDatasets(release_version=self.testparams['release_version'], pset_hash=self.testparams['pset_hash'], \
		app_name=self.testparams['app_name'], output_module_label=self.testparams['output_module_label'])

    def test13(self):
	"""test13 unittestDBSClientReader_t.listDatasets: """
	self.api.listDatasets(dataset=self.testparams['dataset'], release_version=self.testparams['release_version'], \
		pset_hash=self.testparams['pset_hash'], app_name=self.testparams['app_name'], output_module_label=self.testparams['output_module_label'])

    def test14(self):
	"""test14 unittestDBSClientReader_t.listDatasets: """
	self.api.listDatasets(dataset=self.testparams['dataset'], release_version=self.testparams['release_version'])

    def test14a(self):
        """test14a unittestDBSCLientReader_t.listDatasets: using create_by"""
        self.api.listDatasets(create_by='giffels')

    def test14b(self):
        """test14a unittestDBSCLientReader_t.listDatasets: using last_modified_by"""
        self.api.listDatasets(last_modified_by='giffels')

    def test15(self):
	"""test15 unittestDBSClientReader_t.listOutputModules: basic test"""
	self.api.listOutputConfigs(dataset=self.testparams['dataset'])

    def test16(self):
	"""test16 unittestDBSClientReader_t.listOutputModules: """
	self.api.listOutputConfigs(logical_file_name=self.testparams['files'][0])

    def test17(self):
	"""test17 unittestDBSClientReader_t.listOutputModules: """
	self.api.listOutputConfigs()

    def test18(self):
	"""test18 unittestDBSClientReader_t.listOutputModules: """
	self.api.listOutputConfigs(release_version=self.testparams['release_version'])

    def test19(self):
	"""test19 unittestDBSClientReader_t.listOutputModules: """
	self.api.listOutputConfigs(pset_hash=self.testparams['pset_hash'])

    def test20(self):
	"""test20 unittestDBSClientReader_t.listOutputModules: """
	self.api.listOutputConfigs(app_name=self.testparams['app_name'])

    def test21(self):
	"""test21 unittestDBSClientReader_t.listOutputModules: """
	self.api.listOutputConfigs(output_module_label=self.testparams['output_module_label'])

    def test22(self):
	"""test22 unittestDBSClientReader_t.listOutputModules: """
	self.api.listOutputConfigs(release_version=self.testparams['release_version'], pset_hash=self.testparams['pset_hash'], \
		 app_name=self.testparams['app_name'], output_module_label=self.testparams['output_module_label'])

    def test23(self):
	"""test23 unittestDBSClientReader_t.listOutputModules: """
	self.api.listOutputConfigs(dataset=self.testparams['dataset'], release_version=self.testparams['release_version'], \
		pset_hash=self.testparams['pset_hash'], app_name=self.testparams['app_name'], output_module_label=self.testparams['output_module_label'])

    def test24(self):
	"""test24 unittestDBSClientReader_t.listOutputModules: """
	self.api.listOutputConfigs(dataset=self.testparams['dataset'], release_version=self.testparams['release_version'])

    def test25(self):
	"""test25 unittestDBSClientReader_t.listBlocks: basic test"""
	self.api.listBlocks(block_name=self.testparams['block'])

    def test26(self):
	"""test26 unittestDBSClientReader_t.listBlocks: """
	self.api.listBlocks(dataset=self.testparams['dataset'])

    def test27(self):
	"""test27 unittestDBSClientReader_t.listBlocks: """
	self.api.listBlocks(block_name=self.testparams['block'], origin_site_name=self.testparams['site'])

    def test28(self):
	"""test28 unittestDBSClientReader_t.listBlocks: """
	self.api.listBlocks(dataset=self.testparams['dataset'], origin_site_name=self.testparams['site'])

    def test29(self):
	"""test29 unittestDBSClientReader_t.listBlocks: """
	self.api.listBlocks(dataset=self.testparams['dataset'], block_name=self.testparams['block'], origin_site_name=self.testparams['site'])
	try:
	    self.api.listBlocks(origin_site_name=self.testparams['site'])
	except:
	    pass
	else:
	    self.fail("exception was excepted, was not raised")

    def test30(self):
	"""test30 unittestDBSClientReader_t.listDatasetParents basic test"""
	self.api.listDatasetParents(dataset=self.testparams['dataset'])

    def test31(self):
	"""test31 unittestDBSClientReader_t.listDatasetParents basic test"""
	self.api.listDatasetParents(dataset='/does/not/exists')

    def test32(self):
	"""test32 unittestDBSClientReader_t.listFiles: basic test"""
	self.api.listFiles(dataset=self.testparams['dataset'])

    def test33(self):
	"""test33 unittestDBSClientReader_t.listFiles: basic test"""
	self.api.listFiles(block_name=self.testparams['block'])

    def test34(self):
	"""test34 unittestDBSClientReader_t.listFiles: basic test"""
	self.api.listFiles(logical_file_name=self.testparams['files'][0])

    def test35(self):
	"""test35 unittestDBSClientReader_t.listFiles: basic test"""
	self.api.listFiles(dataset=self.testparams['dataset'], release_version=self.testparams['release_version'])

    def test36(self):
	"""test36 unittestDBSClientReader_t.listFiles: basic test"""
	self.api.listFiles(dataset=self.testparams['dataset'], release_version=self.testparams['release_version'], \
		                pset_hash=self.testparams['pset_hash'], app_name=self.testparams['app_name'], output_module_label=self.testparams['output_module_label'])
    def test37(self):
	"""test37 unittestDBSClientReader_t.listFiles: basic test"""
	self.api.listFiles(logical_file_name=self.testparams['files'][0], pset_hash=self.testparams['pset_hash'], app_name=self.testparams['app_name'], output_module_label=self.testparams['output_module_label'])

    def test38(self):
	"""test38 unittestDBSClientReader_t.listFiles: basic test"""
	self.api.listFiles(dataset="/does/not/exist")

    def test39(self):
	"""test39 unittestDBSClientReader_t.listFiles: basic test"""
	self.api.listFiles(block_name="/does/not/exist#123")

    def test40(self):
	"""test40 unittestDBSClientReader_t.listFiles: basic test"""
	self.api.listFiles(logical_file_name="/store/mc/does/not/EXIST/NotReally/0815/doesnotexist.root")

    def test41(self):
	"""test41 unittestDBSClientReader_t.listFileParents: basic test"""
	self.api.listFileParents(logical_file_name=self.testparams['files'][0])

    def test42(self):
	"""test42 unittestDBSClientReader_t.listFileParents: basic test"""
	try:
	    print self.api.listFileParents()
	except:
	    pass
	else:
	    self.fail("exception was excepted, was not raised")

    def test43(self):
	"""test43 unittestDBSClientReader_t.listFileParents: basic test"""
	self.api.listFileParents(logical_file_name="/store/mc/does/not/EXIST/NotReally/0815/doesnotexist.root")

    def test44(self):
	"""test44 unittestDBSClientReader_t.listFileLumis: basic test"""
	self.api.listFileLumis(logical_file_name=self.testparams['files'][0])

    def test45(self):
	"""test45 unittestDBSClientReader_t.listFileLumis: basic test"""
	try:
	    print self.api.listFileLumis()
	except:
	    pass
	else:
	    self.fail("exception was excepted, was not raised")

    def test46(self):
	"""test46 unittestDBSClientReader_t.listFileLumis: basic test"""
	self.api.listFileLumis(logical_file_name="/store/mc/does/not/EXIST/NotReally/0815/doesnotexist.root")

    """
    def test47(self):
	""test47 unittestDBSClientReader_t.listRuns : basic test""
	self.api.listRuns()

    def test48(self):
	""test48 unittestDBSClientReader_t.listRuns : basic test""
	self.api.listRuns(dataset=self.testparams['dataset'])

    def test49(self):
	""test49 unittestDBSClientReader_t.listRuns : basic test""
	self.api.listRuns(block=self.testparams['block'])

    def test50(self):
	""test50 unittestDBSClientReader_t.listRuns : basic test""
	self.api.listRuns(logical_file_name=self.testparams['files'][0])

    def test51(self):
	""test51 unittestDBSClientReader_t.listRuns : basic test""
	self.api.listRuns(dataset=self.testparams['dataset'], block=self.testparams['block'], logical_file_name=self.testparams['files'][0])

    def test52(self):
	""test52 unittestDBSClientReader_t.listRuns : basic test""
	self.api.listRuns(block=self.testparams['block'], logical_file_name=self.testparams['files'][0])

    def test53(self):
	""test53 unittestDBSClientReader_t.listRuns :""
	self.api.listRuns(dataset=self.testparams['dataset'], logical_file_name=self.testparams['files'][0])

    def test54(self):
        ""test54 unittestDBSClientReader_t.listRuns : basic test""
        self.api.listRuns(minrun=self.testparams['runs'][1])

    def test55(self):
        ""test55 unittestDBSClientReader_t.listRuns : basic test""
        self.api.listRuns(maxrun=self.testparams['runs'][2])

    def test55(self):
        ""test55 unittestDBSClientReader_t.listRuns : basic test""
        self.api.listRuns(minrun=self.testparams['runs'][0], maxrun=self.testparams['runs'][2])

    def test56(self):
	""test56 unittestDBSClientReader_t.listRuns : basic test""
	self.api.listRuns(dataset=self.testparams['dataset'], minrun=self.testparams['runs'][0], maxrun=self.testparams['runs'][2])

    def test57(self):
	""test57 unittestDBSClientReader_t.listRuns : basic test""
	self.api.listRuns(block=self.testparams['block'], minrun=self.testparams['runs'][0], maxrun=self.testparams['runs'][2])

    def test58(self):
	""test58 unittestDBSClientReader_t.listRuns : basic test""
	self.api.listRuns(logical_file_name=self.testparams['files'][0], minrun=self.testparams['runs'][0], maxrun=self.testparams['runs'][2])

    def test59(self):
	""test59 unittestDBSClientReader_t.listRuns : basic test""
	self.api.listRuns(dataset=self.testparams['dataset'], block=self.testparams['block'], logical_file_name=self.testparams['files'][0], minrun=self.testparams['runs'][0], maxrun=self.testparams['runs'][2])

    """

    def test60(self):
	"""test60 unittestDBSClientReader_t.listDatasetParents basic test"""
	self.api.listDatasetParents(dataset='/does/not/exists')

    def test61(self):
	"""test61 unittestDBSClientReader_t.listFiles: basic test"""
	self.api.listFiles(dataset=self.testparams['dataset'], minrun=self.testparams['runs'][0], maxrun=self.testparams['runs'][2])

    def test62(self):
	"""test62 unittestDBSClientReader_t.listFiles: basic test"""
	self.api.listFiles(block_name=self.testparams['block'], minrun=self.testparams['runs'][0], maxrun=self.testparams['runs'][2])

    def test63(self):
	"""test63 unittestDBSClientReader_t.listFiles: NOT YET SUPPORTED"""
	#self.api.listFiles(logical_file_name=self.testparams['files'][0], minrun=self.testparams['runs'][0], maxrun=self.testparams['runs'][2])
	pass
    def test64(self):
	"""test64 unittestDBSClientReader_t.listDatasets: processing_version"""
	self.api.listDatasets(dataset=self.testparams['dataset'], processing_version=self.testparams['processing_version'] )

    def test65(self):
	"""test65 unittestDBSClientReader_t.listDatasets: acquisition_era"""
	self.api.listDatasets(dataset=self.testparams['dataset'], acquisition_era_name=self.testparams['acquisition_era'] )

    def test66(self):
	"""test66 unittestDBSClientReader_t.listDatasets: acquisition_era and processing_version both"""
	self.api.listDatasets(dataset=self.testparams['dataset'], acquisition_era_name=self.testparams['acquisition_era'], processing_version=self.testparams['processing_version'] )

    def test67(self):
	"""test67 unittestDBSClientReader_t.listDataTypes: basic test"""
	self.api.listDataTypes()

    def test68(self):
	"""test68 unittestDBSClientReader_t.listDataTypes: for a dataset"""
	self.api.listDataTypes(dataset=self.testparams['dataset'])

    def test69(self):
	"""test69 web.DBSReaderModel.listFile with original site: basic"""
	self.api.listFiles(origin_site_name=self.testparams['site'], dataset=self.testparams['dataset'])

    def test70(self):
	"""test70 web.DBSReaderModel.listFile with original site: basic"""
	self.api.listFiles(origin_site_name=self.testparams['site'], block_name=self.testparams['block'])

    def test71(self):
	"""test71 web.DBSReaderModel.listDatasetParents with dataset"""
	self.api.listDatasetParents(dataset=self.testparams['dataset'])

    def test72(self):
	"""test72 web.DBSReaderModel.listDatasetChildren with dataset"""
	self.api.listDatasetChildren(dataset=self.testparams['parent_dataset'])

    def test73(self):
	"""test73 web.DBSReaderModel.listBlockParents with block"""
	self.api.listBlockParents(block_name=self.testparams['block'])

    def test74(self):
	"""test74 web.DBSReaderModel.listBlockChildren with block_name"""
	self.api.listBlockChildren(block_name=self.testparams['parent_block'])

    def test75(self):
	"""test75 web.DBSReaderModel.listFileParents with logical_file_name"""
	self.api.listFileParents(logical_file_name=self.testparams['files'][0])

    def test76(self):
	"""test76 web.DBSReaderModel.listFileChildren with logical_file_name"""
	self.api.listFileChildren(logical_file_names=self.testparams['parent_files'][0])

    #def test77(self):
        """test77: call help method"""
        #self.api.help()

    #def test78(self):
        """test78 call help method with datatiers"""
        #self.api.help(call="datatiers")
    def test79(self):
        """test79 web.DBSReaderModel.listReleaseVersions"""
        self.api.listReleaseVersions()

    def test80(self):
        """test80 web.DBSReaderModel.listReleaseVersion with release_version=CMSSW_1_2_3"""
        self.api.listReleaseVersions(release_version="CMSSW_1_2_3")

    def test81(self):
        """test81 web.DBSReaderModel.listPhysicsGroups"""
        self.api.listPhysicsGroups()

    def test82(self):
        """test82: web.DBSReaderModel.listPhysicsGroup with physics_group_name"""
        self.api.listPhysicsGroups(physics_group_name="Tracker")

    def test83(self):
        """test83: web.DBSReaderModel.listDatasetAccessType"""
        self.api.listDatasetAccessTypes()

    def test84(self):
        """test84: web.DBSReaderModel.listDatasetAccessType with dataset_access_type"""
        self.api.listDatasetAccessTypes(dataset_access_type="PRODUCTION")

    def test85(self):
        """test85: web.DBSReaderModel.blockDump"""
        try:
            self.api.blockDump()
        except:
            pass
        else:
            self.fail("exception was excepted, was not raised")

    def test86(self):
        """test86: web.DBSReaderModel.blockDump with block_name"""
        self.api.blockDump(block_name=self.testparams["block"])

    def test87(self):
        """test87: web.DBSReaderModel.listPrimaryDSTypes"""
        self.api.listPrimaryDSTypes()

    def test88(self):
        """test88: web.DBSReaderModel.listPrimaryDSTypes with dataset"""
        self.api.listPrimaryDSTypes(dataset=self.testparams['dataset'])

    def test89(self):
        """test89: web.DBSReaderModel.listPrimaryDSTypes with primary_ds_type""" 
        self.api.listPrimaryDSTypes(primary_ds_type="mc")

    def test90(self):
        """test90: web.DBSReaderModel.listDatasetArray"""
        self.api.listDatasetArray(dataset=[self.testparams['dataset']])

    def test91(self):
        """test91: web.DBSReaderModel.listDatasetArray with dataset_access_type"""
        self.api.listDatasetArray(dataset=[self.testparams['dataset']],dataset_access_type="PRODUCTION")

    def test92(self):
        """test92: web.DBSReaderModel.listDatasetArray with detail"""
        self.api.listDatasetArray(dataset=[self.testparams['dataset']], detail=True)

    def test93(self):
        """test93 unittestDBSClientReader_t.listBlockOrigin: """
        self.api.listBlockOrigin(origin_site_name=self.testparams['site'], dataset=self.testparams['dataset'])

    def test94(self):
        """test94 unittestDBSClientReader_t.listBlockOrigin: """
        self.api.listBlockOrigin(origin_site_name=self.testparams['site'])

if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSClientReader_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
