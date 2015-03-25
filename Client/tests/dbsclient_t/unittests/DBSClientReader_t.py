"""
web unittests
"""
import imp
import os
import re
import sys
import unittest

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

    def test006a(self):
        """test06a unittestDBSClientReader_t.listDatasets: """
        self.api.listDatasets(dataset=self.testparams['dataset'])

    def test006b(self):
        """test06b unittestDBSClientReader_t.listDatasets: """
        res = self.api.listDatasets(dataset=self.testparams['dataset'], detail=1)
	self.api.listDatasets(dataset_id=res[0]["dataset_id"], detail=True, dataset_access_type='*')

    def test006c(self):
        """test06c unittestDBSClientReader_t.listDatasets: """
        res = self.api.listDatasets(dataset=self.testparams['dataset'], detail=1)
        self.api.listDatasets(dataset_id=res[0]["dataset_id"], dataset_access_type='*') 

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
        """test14b unittestDBSCLientReader_t.listDatasets: using last_modified_by"""
        self.api.listDatasets(last_modified_by='giffels')

    def test014c(self):
        """test14c unittestDBSCLientReader_t.listDatasets: using run and detail=True"""
        self.api.listDatasets(dataset=self.testparams['dataset'], run_num=self.testparams['runs'], detail=True)

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

    def test029a(self):
        """test29a unittestDBSClientReader_t.listBlocks: """
        self.api.listBlocks(dataset=self.testparams['dataset'], block_name=self.testparams['block'],
                            origin_site_name=self.testparams['site'])
        try:
            self.api.listBlocks(origin_site_name=self.testparams['site'])
        except:
            pass
        else:
            self.fail("exception was excepted, was not raised")

    def test029b(self):
        """test29b unittestDBSClientReader_t.listBlocks: dataset, run_num, detail"""
        self.api.listBlocks(dataset=self.testparams['dataset'], run_num=self.testparams['runs'], detail=True)

    def test030(self):
        """test30 unittestDBSClientReader_t.listDatasetParents basic test"""
        self.api.listDatasetParents(dataset=self.testparams['dataset'])

    def test031(self):
        """test31 unittestDBSClientReader_t.listDatasetParents basic test"""
        self.api.listDatasetParents(dataset='/does/not/EXISTS')

    def test032(self):
        """test32 unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(dataset=self.testparams['dataset'])

    def test032a(self):
        """test32a unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(dataset=self.testparams['dataset'], validFileOnly=1)

    def test032b(self):
        """test32b unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(dataset=self.testparams['dataset'], validFileOnly=0)

    def test033a(self):
        """test33 unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(block_name=self.testparams['block'])

    def test033b(self):
        """test33b unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(block_name=self.testparams['block'], detail=True)

    def test033c(self):
        """test33c unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(block_name=self.testparams['block'], validFileOnly=1)

    def test033d(self):
        """test33d unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(block_name=self.testparams['block'], detail=True, validFileOnly=1)

    def test033e(self):
        """test33e unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(block_name=self.testparams['block'], validFileOnly=0)

    def test033f(self):
        """test33f unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(block_name=self.testparams['block'], detail=True, validFileOnly=0)

    def test033g(self):
        """test033g unittestDBSClientReader_t.listFiles: block, lumi_list and run_num"""
        self.api.listFiles(block_name=self.testparams['block'], run_num=self.testparams['runs'][0], lumi_list=[1,2,3,4,5,6])

    def test033h(self):
        """test033h unittestDBSClientReader_t.listFiles: block, lumi_list and run_num"""
        self.api.listFiles(block_name=self.testparams['block'], run_num=self.testparams['runs'][0], lumi_list=[ [1,20], [30,40], [50,60] ] )

    def test033i(self):
        """test033i unittestDBSClientReader_t.listFiles: block, lumi_list and run_num"""
        self.api.listFiles(block_name=self.testparams['block'], run_num=self.testparams['runs'][0], lumi_list=[1,2,3,4,5,6], detail=1)

    def test033j(self):
        """test033j unittestDBSClientReader_t.listFiles: block, lumi_list and run_num"""
        self.api.listFiles(block_name=self.testparams['block'], run_num=self.testparams['runs'][0], 
		lumi_list=[ [1,20], [30,40], [50,60] ], detail=1 )

    def test033k(self):
        """test033k unittestDBSClientReader_t.listFiles: block, lumi_list and run_num"""
        self.api.listFiles(block_name=self.testparams['block'], run_num=self.testparams['runs'][0], 
		lumi_list=[1,2,3,4,5,6], validFileOnly=1)

    def test033l(self):
        """test033l unittestDBSClientReader_t.listFiles: block, lumi_list and run_num"""
        self.api.listFiles(block_name=self.testparams['block'], run_num=self.testparams['runs'][0], 
		lumi_list=[ [1,20], [30,40], [50,60] ] , validFileOnly=1)

    def test033m(self):
        """test033m unittestDBSClientReader_t.listFiles: block, lumi_list and run_num"""
        self.api.listFiles(block_name=self.testparams['block'], run_num=self.testparams['runs'][0], 
		lumi_list=[1,2,3,4,5,6], detail=1 , validFileOnly=1 )

    def test033n(self):
        """test033n unittestDBSClientReader_t.listFiles: block, lumi_list and run_num"""
        self.api.listFiles(block_name=self.testparams['block'], run_num=self.testparams['runs'][0], 
		lumi_list=[ [1,20], [30,40], [50,60] ], detail=1 , validFileOnly=1 )

    def test033o(self):
        """test033o unittestDBSClientReader_t.listFiles: block, lumi_list and run_num"""
        self.api.listFiles(block_name=self.testparams['block'], run_num=[self.testparams['runs'][0]], lumi_list=[1,2,3,4,5,6])

    def test034a(self):
        """test34a unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(logical_file_name=self.testparams['files'][0])

    def test034b(self):
        """test34b unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(logical_file_name=self.testparams['files'][0], validFileOnly=1)

    def test034c(self):
        """test34c unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(logical_file_name=self.testparams['files'][0], validFileOnly=0)

    def test034d(self):
        """test034d unittestDBSClientReader_t.listFiles: lfn, lumi_list and run_num"""
        self.api.listFiles(logical_file_name=self.testparams['files'][0], run_num=self.testparams['runs'][0], lumi_list=[1,2,3,4,5,6])

    def test034e(self):
        """test040e unittestDBSClientReader_t.listFiles: lfn, lumi_list and run_num"""
        self.api.listFiles(logical_file_name=self.testparams['files'][0], run_num=self.testparams['runs'][0], lumi_list=[ [1,20], [30,40], [50,60] ] )

    def test034f(self):
        """test034f unittestDBSClientReader_t.listFiles: lfn, lumi_list and run_num"""
        self.api.listFiles(logical_file_name=self.testparams['files'][0], run_num=self.testparams['runs'][0], lumi_list=[1,2,3,4,5,6], detail=1)

    def test034g(self):
        """test034g unittestDBSClientReader_t.listFiles: lfn, lumi_list and run_num"""
        self.api.listFiles(logical_file_name=self.testparams['files'][0], run_num=self.testparams['runs'][0], 
		lumi_list=[ [1,20], [30,40], [50,60] ], detail=1 )

    def test034h(self):
        """test034h unittestDBSClientReader_t.listFiles: lfn, lumi_list and run_num"""
        self.api.listFiles(logical_file_name=self.testparams['files'][0], run_num=self.testparams['runs'][0], 
		lumi_list=[1,2,3,4,5,6], validFileOnly=1)

    def test034i(self):
        """test034i unittestDBSClientReader_t.listFiles: lfn, lumi_list and run_num"""
        self.api.listFiles(logical_file_name=self.testparams['files'][0], run_num=self.testparams['runs'][0], 
		lumi_list=[ [1,20], [30,40], [50,60] ] , validFileOnly=1)

    def test034j(self):
        """test034j unittestDBSClientReader_t.listFiles: lfn, lumi_list and run_num"""
        self.api.listFiles(logical_file_name=self.testparams['files'][0], run_num=self.testparams['runs'][0], 
		lumi_list=[1,2,3,4,5,6], detail=1 , validFileOnly=1 )

    def test034k(self):
        """test034k unittestDBSClientReader_t.listFiles: lfn, lumi_list and run_num"""
        self.api.listFiles(logical_file_name=self.testparams['files'][0], run_num=self.testparams['runs'][0], 
		lumi_list=[ [1,20], [30,40], [50,60] ], detail=1 , validFileOnly=1 )

    def test034l(self):
        """test040i unittestDBSClientReader_t.listFiles: lfn, lumi_list and run_num"""
        self.api.listFiles(logical_file_name=self.testparams['files'][0], run_num=[self.testparams['runs'][0]], lumi_list=[1,2,3,4,5,6])

    def test035a(self):
        """test35a unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(dataset=self.testparams['dataset'], release_version=self.testparams['release_version'])

    def test035b(self):
        """test35b unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(dataset=self.testparams['dataset'], release_version=self.testparams['release_version'], validFileOnly=1)

    def test036(self):
        """test36 unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(dataset=self.testparams['dataset'], release_version=self.testparams['release_version'],
                           pset_hash=self.testparams['pset_hash'], app_name=self.testparams['app_name'],
                           output_module_label=self.testparams['output_module_label'])
    def test037a(self):
        """test37a unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(logical_file_name=self.testparams['files'][0], pset_hash=self.testparams['pset_hash'],
                           app_name=self.testparams['app_name'],
                           output_module_label=self.testparams['output_module_label'])
    def test037b(self):
        """test37b unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(logical_file_name=self.testparams['files'][0], pset_hash=self.testparams['pset_hash'],
                           app_name=self.testparams['app_name'],
                           output_module_label=self.testparams['output_module_label'], validFileOnly=1)

    def test038(self):
        """test38 unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(dataset="/does/not/EXISTS")

    def test039(self):
        """test39 unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(block_name="/does/not/EXISTS#123")

    def test040(self):
        """test40 unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(logical_file_name="/store/mc/does/not/EXIST/NotReally/0815/doesnotexist.root")

    def test040a(self):
        """test040a unittestDBSClientReader_t.listFiles: dataset, lumi_list and run_num"""
        self.api.listFiles(dataset=self.testparams['dataset'], run_num=self.testparams['runs'][0], lumi_list=[1,2,3,4,5,6])

    def test040b(self):
        """test040b unittestDBSClientReader_t.listFiles: dataset, lumi_list and run_num"""
        self.api.listFiles(dataset=self.testparams['dataset'], run_num=self.testparams['runs'][0], lumi_list=[ [1,20], [30,40], [50,60] ] )

    def test040c(self):
        """test040c unittestDBSClientReader_t.listFiles: dataset, lumi_list and run_num"""
        self.api.listFiles(dataset=self.testparams['dataset'], run_num=self.testparams['runs'][0], lumi_list=[1,2,3,4,5,6], detail=1)

    def test040d(self):
        """test040d unittestDBSClientReader_t.listFiles: dataset, lumi_list and run_num"""
        self.api.listFiles(dataset=self.testparams['dataset'], run_num=self.testparams['runs'][0], lumi_list=[ [1,20], [30,40], [50,60] ], detail=1 )

    def test040e(self):
        """test040e unittestDBSClientReader_t.listFiles: dataset, lumi_list and run_num"""
        self.api.listFiles(dataset=self.testparams['dataset'], run_num=self.testparams['runs'][0], lumi_list=[1,2,3,4,5,6], validFileOnly=1)

    def test040f(self):
        """test040f unittestDBSClientReader_t.listFiles: dataset, lumi_list and run_num"""
        self.api.listFiles(dataset=self.testparams['dataset'], run_num=self.testparams['runs'][0], 
		lumi_list=[ [1,20], [30,40], [50,60] ] , validFileOnly=1)

    def test040g(self):
        """test040g unittestDBSClientReader_t.listFiles: dataset, lumi_list and run_num"""
        self.api.listFiles(dataset=self.testparams['dataset'], run_num=self.testparams['runs'][0], 
		lumi_list=[1,2,3,4,5,6], detail=1 , validFileOnly=1 )

    def test040h(self):
        """test040h unittestDBSClientReader_t.listFiles: dataset, lumi_list and run_num"""
        self.api.listFiles(dataset=self.testparams['dataset'], run_num=self.testparams['runs'][0], 
		lumi_list=[ [1,20], [30,40], [50,60] ], detail=1 , validFileOnly=1 )

    def test040i(self):
        """test040i unittestDBSClientReader_t.listFiles: dataset, lumi_list and run_num"""
        self.api.listFiles(dataset=self.testparams['dataset'], run_num=[self.testparams['runs'][0]], lumi_list=[1,2,3,4,5,6])

    def test061a(self):
        """test61a unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(dataset=self.testparams['dataset'],
                           run_num='%s-%s' % (self.testparams['runs'][0], self.testparams['runs'][2]))
    def test061b(self):
        """test61b unittestDBSClientReader_t.listFiles: dataset and run_num"""
        self.api.listFiles(dataset=self.testparams['dataset'],
                           run_num=[self.testparams['runs'][0], self.testparams['runs'][2]] )

    def test061c(self):
        """test61c unittestDBSClientReader_t.listFiles: dataset and run_num"""
        self.api.listFiles(dataset=self.testparams['dataset'],
                           run_num=self.testparams['runs'][0])

    def test061d(self):
        """test61d unittestDBSClientReader_t.listFiles: dataset and run_num"""
        self.api.listFiles(dataset=self.testparams['dataset'],
                           run_num=[self.testparams['runs'][2]] )	

    def test061e(self):
        """test61e unittestDBSClientReader_t.listFiles: dataset and run_num"""
        self.api.listFiles(dataset=self.testparams['dataset'],
                           run_num=[str(self.testparams['runs'][2])] )

    def test061f(self):
        """test61f unittestDBSClientReader_t.listFiles: dataset and run_num"""
        self.api.listFiles(dataset=self.testparams['dataset'],
                           run_num=str(self.testparams['runs'][0]) )

    def test062(self):
        """test62 unittestDBSClientReader_t.listFiles: basic test"""
        self.api.listFiles(block_name=self.testparams['block'],
                           run_num=['%s-%s' % (self.testparams['runs'][0], self.testparams['runs'][2])])

    def test063a(self):
        """test63a unittestDBSClientReader_t.listFiles: Mixed run_num range and list of run_nums """
        self.api.listFiles(logical_file_name=self.testparams['files'][0],
                           run_num=['%s-%s' % (self.testparams['runs'][0], self.testparams['runs'][2]), 100, 10000] )

    def test063b(self):
        """test63b unittestDBSClientReader_t.listFiles: Mixed run_num range and list of run_nums """
        self.api.listFiles(logical_file_name=self.testparams['files'][0],
                           run_num=['%s-%s' % (self.testparams['runs'][0], self.testparams['runs'][2]), 100, 10000, '50-100'] )
    
    def test069(self):
        """test69 unittestDBSClientReader_t.listFile with original site: basic"""
        self.api.listFiles(origin_site_name=self.testparams['site'], dataset=self.testparams['dataset'])

    def test070(self):
        """test70 unittestDBSClientReader_t.listFile with original site: basic"""
        self.api.listFiles(origin_site_name=self.testparams['site'], block_name=self.testparams['block'])

    def test03200(self):
        """test03200 unittestDBSClientReader_t.listFileArray: basic test"""
        self.api.listFileArray(dataset=self.testparams['dataset'])

    def test03200a(self):
        """test03200a unittestDBSClientReader_t.listFileArray: basic test"""
        self.api.listFileArray(dataset=self.testparams['dataset'], validFileOnly=1)

    def test03200b(self):
        """test03200b unittestDBSClientReader_t.listFileArray: basic test"""
        self.api.listFileArray(dataset=self.testparams['dataset'], validFileOnly=0)

    def test03300a(self):
        """test03300 unittestDBSClientReader_t.listFileArray: basic test"""
        self.api.listFileArray(block_name=self.testparams['block'])

    def test03300b(self):
        """test03300b unittestDBSClientReader_t.listFileArray: basic test"""
        self.api.listFileArray(block_name=self.testparams['block'], detail=True)

    def test03300c(self):
        """test03300c unittestDBSClientReader_t.listFileArray: basic test"""
        self.api.listFileArray(block_name=self.testparams['block'], validFileOnly=1)

    def test03300d(self):
        """test03300d unittestDBSClientReader_t.listFileArray: basic test"""
        self.api.listFileArray(block_name=self.testparams['block'], detail=True, validFileOnly=1)

    def test03300e(self):
        """test03300e unittestDBSClientReader_t.listFileArray: basic test"""
        self.api.listFileArray(block_name=self.testparams['block'], validFileOnly=0)

    def test03300f(self):
        """test03300f unittestDBSClientReader_t.listFileArray: basic test"""
        self.api.listFileArray(block_name=self.testparams['block'], detail=True, validFileOnly=0)

    def test03300g(self):
        """test03300g unittestDBSClientReader_t.listFileArray: block, lumi_list and run_num"""
        self.api.listFileArray(block_name=self.testparams['block'], run_num=self.testparams['runs'][0], lumi_list=[1,2,3,4,5,6])

    def test03300h(self):
        """test03300h unittestDBSClientReader_t.listFileArray: block, lumi_list and run_num"""
        self.api.listFileArray(block_name=self.testparams['block'], run_num=self.testparams['runs'][0], lumi_list=[ [1,20], [30,40], [50,60] ] )

    def test03300i(self):
        """test03300i unittestDBSClientReader_t.listFileArray: block, lumi_list and run_num"""
        self.api.listFileArray(block_name=self.testparams['block'], run_num=self.testparams['runs'][0], lumi_list=[1,2,3,4,5,6], detail=1)

    def test03300j(self):
        """test03300j unittestDBSClientReader_t.listFileArray: block, lumi_list and run_num"""
        self.api.listFileArray(block_name=self.testparams['block'], run_num=self.testparams['runs'][0], 
		lumi_list=[ [1,20], [30,40], [50,60] ], detail=1 )

    def test03300k(self):
        """test03300k unittestDBSClientReader_t.listFileArray: block, lumi_list and run_num"""
        self.api.listFileArray(block_name=self.testparams['block'], run_num=self.testparams['runs'][0], 
		lumi_list=[1,2,3,4,5,6], validFileOnly=1)

    def test03300l(self):
        """test03300l unittestDBSClientReader_t.listFileArray: block, lumi_list and run_num"""
        self.api.listFileArray(block_name=self.testparams['block'], run_num=self.testparams['runs'][0], 
		lumi_list=[ [1,20], [30,40], [50,60] ] , validFileOnly=1)

    def test03300m(self):
        """test03300m unittestDBSClientReader_t.listFileArray: block, lumi_list and run_num"""
        self.api.listFileArray(block_name=self.testparams['block'], run_num=self.testparams['runs'][0], 
		lumi_list=[1,2,3,4,5,6], detail=1 , validFileOnly=1 )

    def test03300n(self):
        """test03300n unittestDBSClientReader_t.listFileArray: block, lumi_list and run_num"""
        self.api.listFileArray(block_name=self.testparams['block'], run_num=self.testparams['runs'][0], 
		lumi_list=[ [1,20], [30,40], [50,60] ], detail=1 , validFileOnly=1 )

    def test03300o(self):
        """test03300o unittestDBSClientReader_t.listFileArray: block, lumi_list and run_num"""
        self.api.listFileArray(block_name=self.testparams['block'], run_num=[self.testparams['runs'][0]], lumi_list=[1,2,3,4,5,6])

    def test03400a(self):
        """test03400a unittestDBSClientReader_t.listFileArray: basic test"""
        self.api.listFileArray(logical_file_name=self.testparams['files'][0])

    def test03400b(self):
        """test03400b unittestDBSClientReader_t.listFileArray: basic test"""
        self.api.listFileArray(logical_file_name=self.testparams['files'][0], validFileOnly=1)

    def test03400c(self):
        """test03400c unittestDBSClientReader_t.listFileArray: basic test"""
        self.api.listFileArray(logical_file_name=self.testparams['files'][0], validFileOnly=0)

    def test03400d(self):
        """test03400d unittestDBSClientReader_t.listFileArray: lfn, lumi_list and run_num"""
        self.api.listFileArray(logical_file_name=self.testparams['files'][0], run_num=self.testparams['runs'][0], lumi_list=[1,2,3,4,5,6])

    def test03400e(self):
        """test03400e unittestDBSClientReader_t.listFileArray: lfn, lumi_list and run_num"""
        self.api.listFileArray(logical_file_name=self.testparams['files'][0], run_num=self.testparams['runs'][0], lumi_list=[ [1,20], [30,40], [50,60] ] )

    def test03400f(self):
        """test03400f unittestDBSClientReader_t.listFileArray: lfn, lumi_list and run_num"""
        self.api.listFileArray(logical_file_name=self.testparams['files'][0], run_num=self.testparams['runs'][0], lumi_list=[1,2,3,4,5,6], detail=1)

    def test03400g(self):
        """test03400g unittestDBSClientReader_t.listFileArray: lfn, lumi_list and run_num"""
        self.api.listFileArray(logical_file_name=self.testparams['files'][0], run_num=self.testparams['runs'][0], 
		lumi_list=[ [1,20], [30,40], [50,60] ], detail=1 )

    def test03400h(self):
        """test03400h unittestDBSClientReader_t.listFileArray: lfn, lumi_list and run_num"""
        self.api.listFileArray(logical_file_name=self.testparams['files'][0], run_num=self.testparams['runs'][0], 
		lumi_list=[1,2,3,4,5,6], validFileOnly=1)

    def test03400i(self):
        """test03400i unittestDBSClientReader_t.listFileArray: lfn, lumi_list and run_num"""
        self.api.listFileArray(logical_file_name=self.testparams['files'][0], run_num=self.testparams['runs'][0], 
		lumi_list=[ [1,20], [30,40], [50,60] ] , validFileOnly=1)

    def test03400j(self):
        """test03400j unittestDBSClientReader_t.listFileArray: lfn, lumi_list and run_num"""
        self.api.listFileArray(logical_file_name=self.testparams['files'][0], run_num=self.testparams['runs'][0], 
		lumi_list=[1,2,3,4,5,6], detail=1 , validFileOnly=1 )

    def test03400k(self):
        """test03400k unittestDBSClientReader_t.listFileArray: lfn, lumi_list and run_num"""
        self.api.listFileArray(logical_file_name=self.testparams['files'][0], run_num=self.testparams['runs'][0], 
		lumi_list=[ [1,20], [30,40], [50,60] ], detail=1 , validFileOnly=1 )

    def test03400l(self):
        """test03400l unittestDBSClientReader_t.listFileArray: lfn, lumi_list and run_num"""
        self.api.listFileArray(logical_file_name=self.testparams['files'][0], run_num=[self.testparams['runs'][0]], lumi_list=[1,2,3,4,5,6])

    def test03500a(self):
        """test03500a unittestDBSClientReader_t.listFileArray: basic test"""
        self.api.listFileArray(dataset=self.testparams['dataset'], release_version=self.testparams['release_version'])

    def test03500b(self):
        """test3500b unittestDBSClientReader_t.listFileArray: basic test"""
        self.api.listFileArray(dataset=self.testparams['dataset'], release_version=self.testparams['release_version'], validFileOnly=1)

    def test03600(self):
        """test3600 unittestDBSClientReader_t.listFileArray: basic test"""
        self.api.listFileArray(dataset=self.testparams['dataset'], release_version=self.testparams['release_version'],
                           pset_hash=self.testparams['pset_hash'], app_name=self.testparams['app_name'],
                           output_module_label=self.testparams['output_module_label'])
    def test03700a(self):
        """test3700a unittestDBSClientReader_t.listFileArray: basic test"""
        self.api.listFileArray(logical_file_name=self.testparams['files'][0], pset_hash=self.testparams['pset_hash'],
                           app_name=self.testparams['app_name'],
                           output_module_label=self.testparams['output_module_label'])
    def test03700b(self):
        """test3700b unittestDBSClientReader_t.listFileArray: basic test"""
        self.api.listFileArray(logical_file_name=self.testparams['files'][0], pset_hash=self.testparams['pset_hash'],
                           app_name=self.testparams['app_name'],
                           output_module_label=self.testparams['output_module_label'], validFileOnly=1)

    def test03800(self):
        """test03800 unittestDBSClientReader_t.listFileArray: basic test"""
        self.api.listFileArray(dataset="/does/not/EXISTS")

    def test03900(self):
        """test03900 unittestDBSClientReader_t.listFileArray: basic test"""
        self.api.listFileArray(block_name="/does/not/EXISTS#123")

    def test04000(self):
        """test04000 unittestDBSClientReader_t.listFileArray: basic test"""
        self.api.listFileArray(logical_file_name="/store/mc/does/not/EXIST/NotReally/0815/doesnotexist.root")

    def test04000a(self):
        """test04000a unittestDBSClientReader_t.listFileArray: dataset, lumi_list and run_num"""
        self.api.listFileArray(dataset=self.testparams['dataset'], run_num=self.testparams['runs'][0], lumi_list=[1,2,3,4,5,6])

    def test04000b(self):
        """test04000b unittestDBSClientReader_t.listFileArray: dataset, lumi_list and run_num"""
        self.api.listFileArray(dataset=self.testparams['dataset'], run_num=self.testparams['runs'][0], lumi_list=[ [1,20], [30,40], [50,60] ] )

    def test04000c(self):
        """test04000c unittestDBSClientReader_t.listFileArray: dataset, lumi_list and run_num"""
        self.api.listFileArray(dataset=self.testparams['dataset'], run_num=self.testparams['runs'][0], lumi_list=[1,2,3,4,5,6], detail=1)

    def test04000d(self):
        """test04000d unittestDBSClientReader_t.listFileArray: dataset, lumi_list and run_num"""
        self.api.listFileArray(dataset=self.testparams['dataset'], run_num=self.testparams['runs'][0], lumi_list=[ [1,20], [30,40], [50,60] ], detail=1 )

    def test04000e(self):
        """test04000e unittestDBSClientReader_t.listFileArray: dataset, lumi_list and run_num"""
        self.api.listFileArray(dataset=self.testparams['dataset'], run_num=self.testparams['runs'][0], lumi_list=[1,2,3,4,5,6], validFileOnly=1)

    def test04000f(self):
        """test04000f unittestDBSClientReader_t.listFileArray: dataset, lumi_list and run_num"""
        self.api.listFileArray(dataset=self.testparams['dataset'], run_num=self.testparams['runs'][0], 
		lumi_list=[ [1,20], [30,40], [50,60] ] , validFileOnly=1)

    def test04000g(self):
        """test04000g unittestDBSClientReader_t.listFileArray: dataset, lumi_list and run_num"""
        self.api.listFileArray(dataset=self.testparams['dataset'], run_num=self.testparams['runs'][0], 
		lumi_list=[1,2,3,4,5,6], detail=1 , validFileOnly=1 )

    def test04000h(self):
        """test04000h unittestDBSClientReader_t.listFileArray: dataset, lumi_list and run_num"""
        self.api.listFileArray(dataset=self.testparams['dataset'], run_num=self.testparams['runs'][0], 
		lumi_list=[ [1,20], [30,40], [50,60] ], detail=1 , validFileOnly=1 )

    def test04000i(self):
        """test04000i unittestDBSClientReader_t.listFileArray: dataset, lumi_list and run_num"""
        self.api.listFileArray(dataset=self.testparams['dataset'], run_num=[self.testparams['runs'][0]], lumi_list=[1,2,3,4,5,6])

    def test06100a(self):
        """test6100a unittestDBSClientReader_t.listFileArray: basic test"""
        self.api.listFileArray(dataset=self.testparams['dataset'],
                           run_num='%s-%s' % (self.testparams['runs'][0], self.testparams['runs'][2]))
    def test06100b(self):
        """test6100b unittestDBSClientReader_t.listFileArray: dataset and run_num"""
        self.api.listFileArray(dataset=self.testparams['dataset'],
                           run_num=[self.testparams['runs'][0], self.testparams['runs'][2]] )

    def test06100c(self):
        """test6100c unittestDBSClientReader_t.listFileArray: dataset and run_num"""
        self.api.listFileArray(dataset=self.testparams['dataset'],
                           run_num=self.testparams['runs'][0])

    def test06100d(self):
        """test6100d unittestDBSClientReader_t.listFileArray: dataset and run_num"""
        self.api.listFileArray(dataset=self.testparams['dataset'],
                           run_num=[self.testparams['runs'][2]]	)

    def test06100e(self):
        """test06100e unittestDBSClientReader_t.listFileArray: dataset and run_num"""
        self.api.listFileArray(dataset=self.testparams['dataset'],
                           run_num=[str(self.testparams['runs'][2])] )

    def test06100f(self):
        """test06100f unittestDBSClientReader_t.listFileArray: dataset and run_num"""
        self.api.listFileArray(dataset=self.testparams['dataset'],
                           run_num=str(self.testparams['runs'][0]) )

    def test06200(self):
        """test06200 unittestDBSClientReader_t.listFileArray: basic test"""
        self.api.listFileArray(block_name=self.testparams['block'],
                           run_num=['%s-%s' % (self.testparams['runs'][0], self.testparams['runs'][2])])

    def test06300a(self):
        """test06300a unittestDBSClientReader_t.listFileArray: Mixed run_num range and list of run_nums """
        self.api.listFileArray(logical_file_name=self.testparams['files'][0],
                           run_num=['%s-%s' % (self.testparams['runs'][0], self.testparams['runs'][2]), 100, 10000] )

    def test06300b(self):
        """test06300b unittestDBSClientReader_t.listFileArray: Mixed run_num range and list of run_nums """
        self.api.listFileArray(logical_file_name=self.testparams['files'][0],
                           run_num=['%s-%s' % (self.testparams['runs'][0], self.testparams['runs'][2]), 100, 10000, '50-100'] )
    
    def test06900(self):
        """test06900 unittestDBSClientReader_t.listFileArray with original site: basic"""
        self.api.listFileArray(origin_site_name=self.testparams['site'], dataset=self.testparams['dataset'])

    def test07000(self):
        """test07000 unittestDBSClientReader_t.listFile with original site: basic"""
        self.api.listFileArray(origin_site_name=self.testparams['site'], block_name=self.testparams['block'])

    def test034d(self):
        """test34d unittestDBSClientReader_t.listFileArray: basic test"""
        self.api.listFileArray(logical_file_name=[self.testparams['files'][0], self.testparams['files'][1],
			       self.testparams['files'][2],self.testparams['files'][3]],validFileOnly=0)
    def test034e(self):
        """test34e unittestDBSClientReader_t.listFileArray: basic test"""
        self.api.listFileArray(logical_file_name=[self.testparams['files'][0], self.testparams['files'][1],
                               self.testparams['files'][2],self.testparams['files'][3]],validFileOnly=1)
    def test034f(self):
        """test34f unittestDBSClientReader_t.listFileArray: basic test"""
        self.api.listFileArray(logical_file_name=[self.testparams['files'][0], self.testparams['files'][1],
                               self.testparams['files'][2],self.testparams['files'][3]],validFileOnly=1, detail=1)

    def test034g(self):
        """test34g unittestDBSClientReader_t.listFileArray: basic test"""
        self.api.listFileArray(logical_file_name=[self.testparams['files'][0], self.testparams['files'][1],
                               self.testparams['files'][2],self.testparams['files'][3]],detail=1)   
 
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

    def test043a(self):
        """test043a unittestDBSClientReader_t.listFileParents with list of logical_file_name"""
        file_list = [self.testparams['files'][0] for i in xrange(200)]
        self.api.listFileParents(logical_file_name=file_list)

    def test043b(self):
        """test043b unittestDBSClientReader_t.listFileParents with non splitable parameter"""
        file_list = [self.testparams['files'][0] for i in xrange(200)]
        self.assertRaises(dbsClientException,self.api.listFileParents, logical_file_names=file_list)

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

    def test046a(self):
        """test46a unittestDBSClientReader_t.listFileLumis: basic test"""
        self.api.listFileLumis(logical_file_name=self.testparams['files'][0], run_num=self.testparams['runs'][0])

    def test046b(self):
        """test46b unittestDBSClientReader_t.listFileLumis: basic test"""
        self.api.listFileLumis(block_name=self.testparams['block'], run_num=self.testparams['runs'][0])

    def test046c(self):
        """test46c unittestDBSClientReader_t.listFileLumis: basic test"""
        self.api.listFileLumis(logical_file_name="/store/mc/does/not/EXIST/NotReally/0815/doesnotexist.root", validFileOnly=1)

    def test046d(self):
        """test46d unittestDBSClientReader_t.listFileLumis: basic test"""
        self.api.listFileLumis(logical_file_name=self.testparams['files'][0], run_num=self.testparams['runs'][0], validFileOnly=1)

    def test046e(self):
        """test46e unittestDBSClientReader_t.listFileLumis: basic test"""
        self.api.listFileLumis(block_name=self.testparams['block'], run_num=self.testparams['runs'][0], validFileOnly=1)

    def test046f(self):
        """test46f unittestDBSClientReader_t.listFileLumis: basic test"""
        self.api.listFileLumis(block_name=self.testparams['block'], validFileOnly=1)

    def test046a1(self):
        """test046a1 unittestDBSClientReader_t.listFileLumiArray with list of logical_file_name"""
        file_list = [self.testparams['files'][i] for i in xrange(5)]
        self.api.listFileLumiArray(logical_file_name=file_list, validFileOnly=0)

    def test046a2(self):
        """test046a2 unittestDBSClientReader_t.listFileLumiArray with list of logical_file_name"""
        file_list = [self.testparams['files'][i] for i in xrange(5)]
        self.api.listFileLumiArray(logical_file_name=file_list, validFileOnly=1)

    def test046a3(self):
        """ test046a3 unittestDBSClientReader_t.listFileLumiArray with list of logical_file_name"""
        file_list = [self.testparams['files'][i] for i in xrange(5)]
        self.api.listFileLumiArray(logical_file_name=file_list, run_num=[self.testparams['runs'][0]], validFileOnly=0)

    def test046a4(self):
        """ test046a4:  unittestDBSClientReader_t.listFileLumiArray with list of logical_file_name"""
        file_list = [self.testparams['files'][i] for i in xrange(5)]
        self.api.listFileLumiArray(logical_file_name=file_list, run_num=self.testparams['runs'][0], validFileOnly=1)

    def test046aa4(self):
        """ test046aa4:  unittestDBSClientReader_t.listFileLumiArray with list of logical_file_name"""
        file_list = [self.testparams['files'][i] for i in xrange(5)]
	try:
            self.api.listFileLumiArray(logical_file_name=file_list, run_num=[self.testparams['runs'][0],self.testparams['runs'][1]],  validFileOnly=1)
	except:
            pass
        else:
            self.fail("exception was excepted, was not raised")

    def test046a5(self):
        """test046a5 unittestDBSClientReader_t.listFileLumiArray with list of logical_file_name"""
        file = self.testparams['files'][0]
        self.api.listFileLumiArray(logical_file_name=file, run_num=self.testparams['runs'][0], validFileOnly=0)
    
    def test046a6(self):
        """test046a6 unittestDBSClientReader_t.listFileLumiArray with list of logical_file_name"""
        file = self.testparams['files'][0]
        self.api.listFileLumiArray(logical_file_name=file, run_num=self.testparams['runs'][0], validFileOnly=1)

    def test046aa6(self):
        """test046aa6 unittestDBSClientReader_t.listFileLumiArray with list of logical_file_name"""
        file = self.testparams['files'][0]
        self.api.listFileLumiArray(logical_file_name=file, 
		run_num=[self.testparams['runs'][0], self.testparams['runs'][1], '100-200', '300-400'], validFileOnly=1)

    def test046a7(self):
        """test046a7 unittestDBSClientReader_t.listFileLumiArray with list of logical_file_name"""
        file = self.testparams['files'][0]
        self.api.listFileLumiArray(logical_file_name=file, validFileOnly=0)
        
    def test046a8(self):
        """test046a8 unittestDBSClientReader_t.listFileLumiArray with list of logical_file_name"""
        file = self.testparams['files'][0]
        self.api.listFileLumiArray(logical_file_name=file, validFileOnly=1)

    def test046g(self):
        """test46g unittestDBSClientReader_t.listFileSummaries: basic test"""
        self.api.listFileSummaries(dataset=self.testparams['dataset'])

    def test046h(self):
        """test46h unittestDBSClientReader_t.listFileSummaries: basic test"""
        self.api.listFileSummaries(dataset=self.testparams['dataset'], validFileOnly=1 )

    def test046i(self):
        """test46i unittestDBSClientReader_t.listFileSummaries: basic test"""
        self.api.listFileSummaries(dataset=self.testparams['dataset'], run_num=self.testparams['runs'][0], validFileOnly=1)

    def test046j(self):
        """test46j unittestDBSClientReader_t.listFileSummaries: basic test"""
        self.api.listFileSummaries(block_name=self.testparams['block'])

    def test046k(self):
        """test46k unittestDBSClientReader_t.listFileSummaries: basic test"""
        self.api.listFileSummaries(block_name=self.testparams['block'], validFileOnly=1 )

    def test046l(self):
        """test46l unittestDBSClientReader_t.listFileSummaries: basic test"""
        self.api.listFileSummaries(block_name=self.testparams['block'], run_num=self.testparams['runs'][0], validFileOnly=1)

    def test047(self):
        """test47 unittestDBSClientReader_t.listRuns : basic test"""
        self.assertRaises(dbsClientException, self.api.listRuns)

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
        self.api.listRuns(run_num=self.testparams['runs'][1])

    def test055a(self):
        """test55a unittestDBSClientReader_t.listRuns : basic test"""
        self.api.listRuns(run_num=self.testparams['runs'][2])

    def test055b(self):
        """test55b unittestDBSClientReader_t.listRuns : basic test"""
        self.api.listRuns(run_num='%s-%s' % (self.testparams['runs'][0], self.testparams['runs'][2]))

    def test056(self):
        """test56 unittestDBSClientReader_t.listRuns : basic test"""
        self.api.listRuns(dataset=self.testparams['dataset'],
                          run_num=['%s-%s' % (self.testparams['runs'][0], self.testparams['runs'][2])])

    def test057(self):
        """test57 unittestDBSClientReader_t.listRuns : basic test"""
        self.api.listRuns(block_name=self.testparams['block'],
                          run_num='%s-%s' % (self.testparams['runs'][0], self.testparams['runs'][2]))

    def test058(self):
        """test58 unittestDBSClientReader_t.listRuns : basic test"""
        self.api.listRuns(logical_file_name=self.testparams['files'][0],
                          run_num='%s-%s' % (self.testparams['runs'][0], self.testparams['runs'][2]))

    def test059(self):
        """test59 unittestDBSClientReader_t.listRuns : basic test"""
        self.api.listRuns(dataset=self.testparams['dataset'], block_name=self.testparams['block'],
                          logical_file_name=self.testparams['files'][0],
                          run_num=['%s-%s' % (self.testparams['runs'][0], self.testparams['runs'][2])])

    def test060(self):
        """test60 unittestDBSClientReader_t.listDatasetParents basic test"""
        self.api.listDatasetParents(dataset='/does/not/EXISTS')

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
        self.api.listFileChildren(logical_file_name=self.testparams['parent_files'][0])

    def test076a(self):
        """test76a unittestDBSClientReader_t.listFileChildren with list of logical_file_name"""
        file_list = [self.testparams['parent_files'][0] for i in xrange(200)]
        self.api.listFileChildren(logical_file_name=file_list)

    def test076b(self):
        """test76b unittestDBSClientReader_t.listFileChildren with non splitable parameter"""
        file_list = [self.testparams['parent_files'][0] for i in xrange(200)]
        self.assertRaises(dbsClientException, self.api.listFileChildren, logical_file_names=file_list)

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

    def test090a(self):
        """test90a: unittestDBSClientReader_t.listDatasetArray"""
	res=self.api.listDatasets(dataset=self.testparams['dataset'], dataset_access_type="*", detail=1)
        self.api.listDatasetArray(dataset_id=[res[0]['dataset_id']])

    def test091a(self):
        """test91a: unittestDBSClientReader_t.listDatasetArray with dataset_access_type"""
	res1=self.api.listDatasets(dataset=self.testparams['dataset'], dataset_access_type="*", detail=1)
	res2=self.api.listDatasets(dataset=self.testparams['parent_dataset'], dataset_access_type="*", detail=1)
        self.api.listDatasetArray(dataset_id=[res1[0]['dataset_id'], res2[0]['dataset_id']])

    def test092a(self):
        """test92a: unittestDBSClientReader_t.listDatasetArray with detail"""
        self.api.listDatasetArray(dataset=[self.testparams['dataset']], detail=True)

    def test093(self):
        """test93 unittestDBSClientReader_t.listBlockOrigin: """
        self.api.listBlockOrigin(origin_site_name=self.testparams['site'], dataset=self.testparams['dataset'])

    def test093b(self):
        """test93b unittestDBSClientReader_t.listBlockOrigin: """
        self.api.listBlockOrigin(dataset=self.testparams['dataset'])

    def test093c(self):
        """test93c unittestDBSClientReader_t.listBlockOrigin: """
        self.api.listBlockOrigin(block_name=self.testparams['block'])

    def test093d(self):
        """test93d unittestDBSClientReader_t.listBlockOrigin: """
        self.api.listBlockOrigin(origin_site_name=self.testparams['site'], block_name=self.testparams['block'])

    def test094(self):
        """test94 unittestDBSClientReader_t.listBlockOrigin: """
        try:
                self.api.listBlockOrigin(origin_site_name=self.testparams['site'])
        except dbsClientException as e:
                if "Invalid input:" in str(e):
                        pass

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
    
    def test099a(self):
        """test099a: unittestDBSClientReader_t.listBlockSummaries: simple block with detail example"""
        self.api.listBlockSummaries(block_name=self.testparams['block'], detail=True)

    def test100a(self):
        """test100a: unittestDBSClientReader_t.listBlockSummaries: simple dataset with detail example"""
        self.api.listBlockSummaries(dataset=self.testparams['dataset'], detail=True)

    def test101a(self):
        """test101a: unittestDBSClientReader_t.listBlockSummaries: simple block_list with detail example"""
        self.api.listBlockSummaries(block_name=[self.testparams['block'], self.testparams['block']], detail=True)

    def test102(self):
        """test102: unittestDBSClientReader_t.listRunSummaries: input validation test"""
        self.assertRaises(dbsClientException, self.api.listRunSummaries)

    def test103(self):
        """test103: unittestDBSClientReader_t.listRunSummaries: input validation test"""
        self.assertRaises(dbsClientException, self.api.listRunSummaries, dataset=self.testparams['dataset'])

    def test104(self):
        """test104: unittestDBSClientReader_t.listRunSummaries: input validation test"""
        self.assertRaises(HTTPError, self.api.listRunSummaries, dataset='/A/B*/C',
                          run_num=self.testparams['runs'][0])

    def test105(self):
        """test105: unittestDBSClientReader_t.listRunSummaries: simple run example"""
        self.api.listRunSummaries(run_num=self.testparams['runs'][0])

    def test106(self):
        """test106: unittestDBSClientReader_t.listRunSummaries: simple run and dataset example"""
        self.api.listRunSummaries(dataset=self.testparams['dataset'], run_num=self.testparams['runs'][0])

    def test107(self):
        """test107: unittestDBSClientReader_t.serverinfo: get server info"""
        reg_ex = r'^(3+\.[0-9]+\.[0-9]+[\.\-a-z0-9]*$)'
        version = self.api.serverinfo()
        self.assertTrue(version.has_key('dbs_version'))
        self.assertFalse(re.compile(reg_ex).match(version['dbs_version']) is None)

if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSClientReader_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
