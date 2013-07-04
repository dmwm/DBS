"""
web unittests
"""
import os
import unittest
import sys,imp

from dbs.apis.dbsClient import *
from dbs.exceptions.dbsClientException import dbsClientException
from RestClient.ErrorHandling.RestClientExceptions import HTTPError


def importCode(code, name, add_to_sys_modules=0):
    module = imp.new_module(name)
    exec code in module.__dict__
    if add_to_sys_modules:
        sys.modules[name] = module
    return module


class DBSClientReader_t(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super(DBSClientReader_t, self).__init__(methodName)
        url = os.environ['DBS_WRITER_URL']
        proxy = os.environ.get('SOCKS5_PROXY')
        self.api = DbsApi(url=url, proxy=proxy)

    def setUp(self):
        """setup all necessary parameters"""
        infofile = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "info.dict"), "r")
        self.testparams = importCode(infofile, "testparams", 0).info

    def test000a(self):
        """test00 unittestDBSClientReader_t.requestTimingInfo"""
        self.api.requestTimingInfo

    def test000b(self):
        """test00b unittestDBSClientReader_t.requestContentLength"""
        self.api.requestContentLength

    def test001(self):
        """test01 unittestDBSClientReader_t.listPrimaryDatasets: basic test"""
        self.api.listPrimaryDatasets()

    def test002(self):
        """test02 unittestDBSClientReader_t.listPrimaryDatasets: """
        self.api.listPrimaryDatasets(primary_ds_name='*')

    def test003(self):
        """test03 unittestDBSClientReader_t.listPrimaryDatasets: """
        self.api.listPrimaryDatasets(primary_ds_name=self.testparams['primary_ds_name'])

    def test004(self):
        """test04 unittestDBSClientReader_t.listPrimaryDatasets: """
        self.api.listPrimaryDatasets(primary_ds_name=self.testparams['primary_ds_name']+"*")

    def test005(self):
        """test05 unittestDBSClientReader_t.listDatasets: basic test"""
        self.api.listDatasets()

    def test006(self):
        """test06 unittestDBSClientReader_t.listDatasets: """
        self.api.listDatasets(dataset=self.testparams['dataset'])

    def test007(self):
        """test07 unittestDBSClientReader_t.listDatasets: """
        self.api.listDatasets(dataset=self.testparams['dataset']+"*")

    def test008(self):
        """test08 unittestDBSClientReader_t.listDatasets: """
        self.api.listDatasets(release_version=self.testparams['release_version'])

    def test009(self):
        """test09 unittestDBSClientReader_t.listDatasets: """
        self.api.listDatasets(pset_hash=self.testparams['pset_hash'])

    def test010(self):
        """test10 unittestDBSClientReader_t.listDatasets: """
        self.api.listDatasets(app_name=self.testparams['app_name'])

    def test011(self):
        """test11 unittestDBSClientReader_t.listDatasets: """
        self.api.listDatasets(output_module_label=self.testparams['output_module_label'])

    def test012(self):
        """test12 unittestDBSClientReader_t.listDatasets: """
        self.api.listDatasets(release_version=self.testparams['release_version'],
                              pset_hash=self.testparams['pset_hash'], app_name=self.testparams['app_name'],
                              output_module_label=self.testparams['output_module_label'])

    def test013(self):
        """test13 unittestDBSClientReader_t.listDatasets: """
        self.api.listDatasets(dataset=self.testparams['dataset'], release_version=self.testparams['release_version'],
                              pset_hash=self.testparams['pset_hash'], app_name=self.testparams['app_name'],
                              output_module_label=self.testparams['output_module_label'])

    def test014(self):
        """test14 unittestDBSClientReader_t.listDatasets: """
        self.api.listDatasets(dataset=self.testparams['dataset'], release_version=self.testparams['release_version'])

    def test014a(self):
        """test14a unittestDBSCLientReader_t.listDatasets: using create_by"""
        self.api.listDatasets(create_by='giffels')

    def test014b(self):
        """test14a unittestDBSCLientReader_t.listDatasets: using last_modified_by"""
        self.api.listDatasets(last_modified_by='giffels')

    def test015(self):
        """test15 unittestDBSClientReader_t.listOutputModules: basic test"""
        self.api.listOutputConfigs(dataset=self.testparams['dataset'])

    def test016(self):
        """test16 unittestDBSClientReader_t.listOutputModules: """
        self.api.listOutputConfigs(logical_file_name=self.testparams['files'][0])

    def test017(self):
        """test17 unittestDBSClientReader_t.listOutputModules: """
        self.api.listOutputConfigs()

    def test018(self):
        """test18 unittestDBSClientReader_t.listOutputModules: """
        self.api.listOutputConfigs(release_version=self.testparams['release_version'])

    def test019(self):
        """test19 unittestDBSClientReader_t.listOutputModules: """
        self.api.listOutputConfigs(pset_hash=self.testparams['pset_hash'])

    def test020(self):
        """test20 unittestDBSClientReader_t.listOutputModules: """
        self.api.listOutputConfigs(app_name=self.testparams['app_name'])

    def test021(self):
        """test21 unittestDBSClientReader_t.listOutputModules: """
        self.api.listOutputConfigs(output_module_label=self.testparams['output_module_label'])

    def test022(self):
        """test22 unittestDBSClientReader_t.listOutputModules: """
        self.api.listOutputConfigs(release_version=self.testparams['release_version'],
                                   pset_hash=self.testparams['pset_hash'], app_name=self.testparams['app_name'],
                                   output_module_label=self.testparams['output_module_label'])

    def test023(self):
        """test23 unittestDBSClientReader_t.listOutputModules: """
        self.api.listOutputConfigs(dataset=self.testparams['dataset'],
                                   release_version=self.testparams['release_version'],
                                   pset_hash=self.testparams['pset_hash'], app_name=self.testparams['app_name'],
                                   output_module_label=self.testparams['output_module_label'])

    def test024(self):
        """test24 unittestDBSClientReader_t.listOutputModules: """
        self.api.listOutputConfigs(dataset=self.testparams['dataset'],
                                   release_version=self.testparams['release_version'])

    def test025(self):
        """test25 unittestDBSClientReader_t.listBlocks: basic test"""
        self.api.listBlocks(block_name=self.testparams['block'])

    def test026(self):
        """test26 unittestDBSClientReader_t.listBlocks: """
        self.api.listBlocks(dataset=self.testparams['dataset'])

    def test027(self):
        """test27 unittestDBSClientReader_t.listBlocks: """
        self.api.listBlocks(block_name=self.testparams['block'], origin_site_name=self.testparams['site'])

    def test028(self):
        """test28 unittestDBSClientReader_t.listBlocks: """
        self.api.listBlocks(dataset=self.testparams['dataset'], origin_site_name=self.testparams['site'])

    def test029(self):
        """test29 unittestDBSClientReader_t.listBlocks: """
        self.api.listBlocks(dataset=self.testparams['dataset'], block_name=self.testparams['block'],
                            origin_site_name=self.testparams['site'])
        try:
            self.api.listBlocks(origin_site_name=self.testparams['site'])
        except:
            pass
        else:
            self.fail("exception was excepted, was not raised")

    def test030(self):
        """test30 unittestDBSClientReader_t.listDatasetParents basic test"""
        self.api.listDatasetParents(dataset=self.testparams['dataset'])

    def test031(self):
        """test31 unittestDBSClientReader_t.listDatasetParents basic test"""
        self.api.listDatasetParents(dataset='/does/not/exists')

    def test032(self):
        """test32 unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(dataset=self.testparams['dataset'])

    def test033(self):
        """test33 unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(block_name=self.testparams['block'])

    def test034(self):
        """test34 unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(logical_file_name=self.testparams['files'][0])

    def test035(self):
        """test35 unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(dataset=self.testparams['dataset'], release_version=self.testparams['release_version'])

    def test036(self):
        """test36 unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(dataset=self.testparams['dataset'], release_version=self.testparams['release_version'],
                           pset_hash=self.testparams['pset_hash'], app_name=self.testparams['app_name'],
                           output_module_label=self.testparams['output_module_label'])
    def test037(self):
        """test37 unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(logical_file_name=self.testparams['files'][0], pset_hash=self.testparams['pset_hash'],
                           app_name=self.testparams['app_name'],
                           output_module_label=self.testparams['output_module_label'])

    def test038(self):
        """test38 unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(dataset="/does/not/exist")

    def test039(self):
        """test39 unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(block_name="/does/not/exist#123")

    def test040(self):
        """test40 unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(logical_file_name="/store/mc/does/not/EXIST/NotReally/0815/doesnotexist.root")

    def test041(self):
        """test41 unittestDBSClientReader_t.listFileParents: basic test"""
        self.api.listFileParents(logical_file_name=self.testparams['files'][0])

    def test042(self):
        """test42 unittestDBSClientReader_t.listFileParents: basic test"""
        try:
            print self.api.listFileParents()
        except:
            pass
        else:
            self.fail("exception was excepted, was not raised")

    def test043(self):
        """test43 unittestDBSClientReader_t.listFileParents: basic test"""
        self.api.listFileParents(logical_file_name="/store/mc/does/not/EXIST/NotReally/0815/doesnotexist.root")

    def test044(self):
        """test44 unittestDBSClientReader_t.listFileLumis: basic test"""
        self.api.listFileLumis(logical_file_name=self.testparams['files'][0])

    def test045(self):
        """test45 unittestDBSClientReader_t.listFileLumis: basic test"""
        try:
            print self.api.listFileLumis()
        except:
            pass
        else:
            self.fail("exception was excepted, was not raised")

    def test046(self):
        """test46 unittestDBSClientReader_t.listFileLumis: basic test"""
        self.api.listFileLumis(logical_file_name="/store/mc/does/not/EXIST/NotReally/0815/doesnotexist.root")


    def test047(self):
        """test47 unittestDBSClientReader_t.listRuns : basic test"""
        self.api.listRuns()

    def test048(self):
        """test48 unittestDBSClientReader_t.listRuns : basic test"""
        self.api.listRuns(dataset=self.testparams['dataset'])

    def test049(self):
        """test49 unittestDBSClientReader_t.listRuns : basic test"""
        self.api.listRuns(block_name=self.testparams['block'])

    def test050(self):
        """test50 unittestDBSClientReader_t.listRuns : basic test"""
        self.api.listRuns(logical_file_name=self.testparams['files'][0])

    def test051(self):
        """test51 unittestDBSClientReader_t.listRuns : basic test"""
        self.api.listRuns(dataset=self.testparams['dataset'], block_name=self.testparams['block'],
                          logical_file_name=self.testparams['files'][0])

    def test052(self):
        """test52 unittestDBSClientReader_t.listRuns : basic test"""
        self.api.listRuns(block_name=self.testparams['block'], logical_file_name=self.testparams['files'][0])

    def test053(self):
        """test53 unittestDBSClientReader_t.listRuns :"""
        self.api.listRuns(dataset=self.testparams['dataset'], logical_file_name=self.testparams['files'][0])

    def test054(self):
        """test54 unittestDBSClientReader_t.listRuns : basic test"""
        self.api.listRuns(run=self.testparams['runs'][1])

    def test055(self):
        """test55 unittestDBSClientReader_t.listRuns : basic test"""
        self.api.listRuns(run=self.testparams['runs'][2])

    def test055(self):
        """test55 unittestDBSClientReader_t.listRuns : basic test"""
        self.api.listRuns(run='%s-%s' % (self.testparams['runs'][0], self.testparams['runs'][2]))

    def test056(self):
        """test56 unittestDBSClientReader_t.listRuns : basic test"""
        self.api.listRuns(dataset=self.testparams['dataset'],
                          run=['%s-%s' % (self.testparams['runs'][0], self.testparams['runs'][2])])

    def test057(self):
        """test57 unittestDBSClientReader_t.listRuns : basic test"""
        self.api.listRuns(block_name=self.testparams['block'],
                          run='%s-%s' % (self.testparams['runs'][0], self.testparams['runs'][2]))

    def test058(self):
        """test58 unittestDBSClientReader_t.listRuns : basic test"""
        self.api.listRuns(logical_file_name=self.testparams['files'][0],
                          run='%s-%s' % (self.testparams['runs'][0], self.testparams['runs'][2]))

    def test059(self):
        """test59 unittestDBSClientReader_t.listRuns : basic test"""
        self.api.listRuns(dataset=self.testparams['dataset'], block_name=self.testparams['block'],
                          logical_file_name=self.testparams['files'][0],
                          run=['%s-%s' % (self.testparams['runs'][0], self.testparams['runs'][2])])

    def test060(self):
        """test60 unittestDBSClientReader_t.listDatasetParents basic test"""
        self.api.listDatasetParents(dataset='/does/not/exists')

    def test061(self):
        """test61 unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(dataset=self.testparams['dataset'],
                           run='%s-%s' % (self.testparams['runs'][0], self.testparams['runs'][2]))

    def test062(self):
        """test62 unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(block_name=self.testparams['block'],
                           run=['%s-%s' % (self.testparams['runs'][0], self.testparams['runs'][2])])

    def test063(self):
        """test63 unittestDBSClientReader_t.listFiles: NOT YET SUPPORTED"""
        self.api.listFiles(logical_file_name=self.testparams['files'][0],
                           run='%s-%s' % (self.testparams['runs'][0], self.testparams['runs'][2]))

    def test064(self):
        """test64 unittestDBSClientReader_t.listDatasets: processing_version"""
        self.api.listDatasets(dataset=self.testparams['dataset'],
                              processing_version=self.testparams['processing_version'] )

    def test065(self):
        """test65 unittestDBSClientReader_t.listDatasets: acquisition_era"""
        self.api.listDatasets(dataset=self.testparams['dataset'],
                              acquisition_era_name=self.testparams['acquisition_era'] )

    def test066(self):
        """test66 unittestDBSClientReader_t.listDatasets: acquisition_era and processing_version both"""
        self.api.listDatasets(dataset=self.testparams['dataset'],
                              acquisition_era_name=self.testparams['acquisition_era'],
                              processing_version=self.testparams['processing_version'] )

    def test067(self):
        """test67 unittestDBSClientReader_t.listDataTypes: basic test"""
        self.api.listDataTypes()

    def test068(self):
        """test68 unittestDBSClientReader_t.listDataTypes: for a dataset"""
        self.api.listDataTypes(dataset=self.testparams['dataset'])

    def test069(self):
        """test69 unittestDBSClientReader_t.listFile with original site: basic"""
        self.api.listFiles(origin_site_name=self.testparams['site'], dataset=self.testparams['dataset'])

    def test070(self):
        """test70 unittestDBSClientReader_t.listFile with original site: basic"""
        self.api.listFiles(origin_site_name=self.testparams['site'], block_name=self.testparams['block'])

    def test071(self):
        """test71 unittestDBSClientReader_t.listDatasetParents with dataset"""
        self.api.listDatasetParents(dataset=self.testparams['dataset'])

    def test072(self):
        """test72 unittestDBSClientReader_t.listDatasetChildren with dataset"""
        self.api.listDatasetChildren(dataset=self.testparams['parent_dataset'])

    def test073(self):
        """test73 unittestDBSClientReader_t.listBlockParents with block"""
        self.api.listBlockParents(block_name=self.testparams['block'])

    def test074(self):
        """test74 unittestDBSClientReader_t.listBlockChildren with block_name"""
        self.api.listBlockChildren(block_name=self.testparams['parent_block'])

    def test075(self):
        """test75 unittestDBSClientReader_t.listFileParents with logical_file_name"""
        self.api.listFileParents(logical_file_name=self.testparams['files'][0])

    def test076(self):
        """test76 unittestDBSClientReader_t.listFileChildren with logical_file_name"""
        self.api.listFileChildren(logical_file_names=self.testparams['parent_files'][0])

    def test076a(self):
        """test76a unittestDBSClientReader_t.listFileChildren with list of logical_file_names"""
        file_list = [self.testparams['parent_files'][0] for i in xrange(200)]
        self.api.listFileChildren(logical_file_names=file_list)

    def test076b(self):
        """test76b unittestDBSClientReader_t.listFileChildren with non splitable parameter"""
        file_list = [self.testparams['parent_files'][0] for i in xrange(200)]
        self.assertRaises(dbsClientException,self.api.listFileChildren, logical_file_name=file_list)

    def test076c(self):
        """test76c unittestDBSClientReader_t.listFileChildren with block_name"""
        self.api.listFileChildren(block_name=self.testparams['block'])

    def test076d(self):
        """test76d unittestDBSClientReader_t.listFileChildren with block_id"""
        self.api.listFileChildren(block_id=123)

    def test077(self):
        """test77: call help method"""
        self.api.help()

    #def test078(self):
    #    """test78 call help method with datatiers"""
    #    self.api.help(call="datatiers")

    def test079(self):
        """test79 unittestDBSClientReader_t.listReleaseVersions"""
        self.api.listReleaseVersions()

    def test080(self):
        """test80 unittestDBSClientReader_t.listReleaseVersion with release_version=CMSSW_1_2_3"""
        self.api.listReleaseVersions(release_version="CMSSW_1_2_3")

    def test081(self):
        """test81 unittestDBSClientReader_t.listPhysicsGroups"""
        self.api.listPhysicsGroups()

    def test082(self):
        """test82: unittestDBSClientReader_t.listPhysicsGroup with physics_group_name"""
        self.api.listPhysicsGroups(physics_group_name="Tracker")

    def test083(self):
        """test83: unittestDBSClientReader_t.listDatasetAccessType"""
        self.api.listDatasetAccessTypes()

    def test084(self):
        """test84: unittestDBSClientReader_t.listDatasetAccessType with dataset_access_type"""
        self.api.listDatasetAccessTypes(dataset_access_type="PRODUCTION")

    def test085(self):
        """test85: unittestDBSClientReader_t.blockDump"""
        try:
            self.api.blockDump()
        except:
            pass
        else:
            self.fail("exception was excepted, was not raised")

    def test086(self):
        """test86: unittestDBSClientReader_t.blockDump with block_name"""
        self.api.blockDump(block_name=self.testparams["block"])

    def test087(self):
        """test87: unittestDBSClientReader_t.listPrimaryDSTypes"""
        self.api.listPrimaryDSTypes()

    def test088(self):
        """test88: unittestDBSClientReader_t.listPrimaryDSTypes with dataset"""
        self.api.listPrimaryDSTypes(dataset=self.testparams['dataset'])

    def test089(self):
        """test89: unittestDBSClientReader_t.listPrimaryDSTypes with primary_ds_type"""
        self.api.listPrimaryDSTypes(primary_ds_type="mc")

    def test090(self):
        """test90: unittestDBSClientReader_t.listDatasetArray"""
        self.api.listDatasetArray(dataset=[self.testparams['dataset']])

    def test091(self):
        """test91: unittestDBSClientReader_t.listDatasetArray with dataset_access_type"""
        self.api.listDatasetArray(dataset=[self.testparams['dataset']],dataset_access_type="PRODUCTION")

    def test092(self):
        """test92: unittestDBSClientReader_t.listDatasetArray with detail"""
        self.api.listDatasetArray(dataset=[self.testparams['dataset']], detail=True)

    def test093(self):
        """test93 unittestDBSClientReader_t.listBlockOrigin: """
        self.api.listBlockOrigin(origin_site_name=self.testparams['site'], dataset=self.testparams['dataset'])

    def test094(self):
        """test94 unittestDBSClientReader_t.listBlockOrigin: """
        self.api.listBlockOrigin(origin_site_name=self.testparams['site'])

    def test095(self):
        """test095: unittestDBSClientReader_t.listBlockSummaries: input validation test"""
        self.assertRaises(dbsClientException, self.api.listBlockSummaries)

    def test096(self):
        """test096: unittestDBSClientReader_t.listBlockSummaries: input validation test"""
        self.assertRaises(HTTPError, self.api.listBlockSummaries, block_name='/*/B/C#abcdef12345')

    def test097(self):
        """test097: unittestDBSClientReader_t.listBlockSummaries: input validation test"""
        self.assertRaises(HTTPError, self.api.listBlockSummaries, dataset='/*/B/C')

    def test098(self):
        """test098: unittestDBSClientReader_t.listBlockSummaries: input validation test"""
        self.assertRaises(dbsClientException, self.api.listBlockSummaries, dataset=self.testparams['dataset'],
                          block_name=self.testparams['block'])

    def test099(self):
        """test099: unittestDBSClientReader_t.listBlockSummaries: simple block example"""
        self.api.listBlockSummaries(block_name=self.testparams['block'])

    def test100(self):
        """test100: unittestDBSClientReader_t.listBlockSummaries: simple dataset example"""
        self.api.listBlockSummaries(dataset=self.testparams['dataset'])

    def test101(self):
        """test101: unittestDBSClientReader_t.listBlockSummaries: simple block_list example"""
        self.api.listBlockSummaries(block_name=[self.testparams['block'], self.testparams['block']])

    def test102(self):
        """test102: unittestDBSClientReader_t.listRunSummaries: input validation test"""
        self.assertRaises(dbsClientException, self.api.listRunSummaries)

    def test103(self):
        """test103: unittestDBSClientReader_t.listRunSummaries: input validation test"""
        self.assertRaises(dbsClientException, self.api.listRunSummaries, dataset=self.testparams['dataset'])

    def test104(self):
        """test104: unittestDBSClientReader_t.listRunSummaries: input validation test"""
        self.assertRaises(HTTPError, self.api.listRunSummaries, dataset='/A/B*/C',
                          run=self.testparams['runs'][0])

    def test105(self):
        """test105: unittestDBSClientReader_t.listRunSummaries: simple run example"""
        self.api.listRunSummaries(run=self.testparams['runs'][0])

    def test106(self):
        """test106: unittestDBSClientReader_t.listRunSummaries: simple run and dataset example"""
        self.api.listRunSummaries(dataset=self.testparams['dataset'], run=self.testparams['runs'][0])

if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSClientReader_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
