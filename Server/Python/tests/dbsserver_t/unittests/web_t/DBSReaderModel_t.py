"""
web unittests
"""
from __future__ import absolute_import
import imp
import os
import re
import sys
import unittest
import time
from functools import wraps

from dbsserver_t.utils.DBSRestApi import DBSRestApi
from .DBSWriterModel_t import outDict
from dbs.utils.dbsException import dbsException, dbsExceptionCode


def checkException400(f):
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        out = None
        try:
            out = f(self, *args, **kwargs)
        except Exception as ex:
            if 'HTTPError 400' not in ex.args[0]:
                self.fail("Exception was expected and was not raised.")
        else:
            self.fail("Exception was expected and was not raised.")
        return out
    return wrapper

def importCode(code, name, add_to_sys_modules=0):
    module = imp.new_module(name)
    exec code in module.__dict__
    if add_to_sys_modules:
        sys.modules[name] = module
    return module

config = os.environ["DBS_TEST_CONFIG"]
service = os.environ.get("DBS_TEST_SERVICE", "DBSReader")
api = DBSRestApi(config, service)


class DBSReaderModel_t(unittest.TestCase):
    def setUp(self):
        """setup all necessary parameters"""
        global testparams
        filename=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'info.dict')
        infofile=open(filename, "r")
        testparams=importCode(infofile, "testparams", 0).info

        if len(testparams) == 0:
            testparams = outDict
        self._current_unixtime = int(time.time())

    def test001a(self):
        """test001a: web.DBSReaderModel.listPrimaryDatasets: basic test"""
        api.list('primarydatasets')

    def test001b(self):
        """test001b: web.DBSReaderModel.listPrimaryDatasets: basic test"""
        api.list('primarydatasets', primary_ds_name='*')

    def test001c(self):
        """test001c: web.DBSReaderModel.listPrimaryDatasets: basic test"""
        api.list('primarydatasets', primary_ds_name=testparams['primary_ds_name'])

    def test001d(self):
        """test001d: web.DBSReaderModel.listPrimaryDatasets: basic test"""
        api.list('primarydatasets', primary_ds_name=testparams['primary_ds_name']+'*')

    def test001e(self):
        """test001e: web.DBSReaderModel.listPrimaryDatasets: basic test"""
        api.list('primarydatasets', primary_ds_type=testparams['primary_ds_type'])

    def test001f(self):
        """test001f: web.DBSReaderModel.listPrimaryDatasets: basic test"""
        api.list('primarydatasets', primary_ds_type=testparams['primary_ds_type']+'*')

    def test002a(self):
        """test002a: web.DBSReaderModel.listPrimaryDsTypes: basic test"""
        api.list('primarydstypes')

    def test002b(self):
        """test002b: web.DBSReaderModel.listPrimaryDsTypes: basic test"""
        api.list('primarydstypes', primary_ds_type=testparams['primary_ds_type'])

    def test002c(self):
        """test002c: web.DBSReaderModel.listPrimaryDsTypes: basic test"""
        api.list('primarydstypes', dataset=testparams['dataset'])

    def test002d(self):
        """test002d: web.DBSReaderModel.listPrimaryDsTypes: basic test"""
        api.list('primarydstypes', primary_ds_type=testparams['primary_ds_type'], dataset=testparams['dataset'])

    def test002e(self):
        """test002e: web.DBSReaderModel.listPrimaryDsTypes: basic test"""
        api.list('primarydstypes', dataset=testparams['dataset']+'*')

    def test002f(self):
        """test002f: web.DBSReaderModel.listPrimaryDsTypes: basic test"""
        api.list('primarydstypes', primary_ds_type=testparams['primary_ds_type']+'*')

    def test003a(self):
        """test003a: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets')

    def test003b(self):
        """test003b: web.DBSReaderModel.listDatasets: detail"""
        api.list('datasets', detail=True)

    def test003c(self):
        """test003c: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', dataset='/*')

    def test003d(self):
        """test003d: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', run_num=str(testparams['run_num']))

    def test003e(self):
        """test003e: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', dataset=testparams['dataset'], dataset_access_type='*')

    def test003f(self):
        """test003f: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', primary_ds_type=testparams['primary_ds_type'])

    def test003g(self):
        """test003g: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets',  primary_ds_name=testparams['primary_ds_name'])

    def test003h(self):
        """test003h: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', dataset=testparams['dataset']+'*')

    def test003i(self):
        """test003i: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', parent_dataset='*')

    def test003j(self):
        """test003j: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', data_tier_name='*RAW*')

    def test003k(self):
        """test003k: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', release_version='*')

    def test003l(self):
        """test003l: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', physics_group_name='QCD')

    def test003m(self):
        """test003m: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', release_version=testparams['release_version'])

    def test003n(self):
        """test003n: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', dataset_access_type='READONLY')

    def test003o(self):
        """test003o: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', release_version=testparams['release_version']+'*')

    def test003p(self):
        """test003p: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', logical_file_name=testparams['files'][0])

    def test003q(self):
        """test003q: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', pset_hash='*')

    def test003r(self):
        """test003r: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', pset_hash=testparams['pset_hash'])

    def test003s(self):
        """test003s: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', app_name='*')

    def test003t(self):
        """test003t: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', app_name=testparams['app_name'])

    def test003u(self):
        """test003u: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', output_module_label='*')

    def test003v(self):
        """test003v: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', output_module_label=testparams['output_module_label'])

    def test003ua(self):
        """test003u: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', global_tag='*')

    def test003va(self):
        """test003v: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', global_tag=testparams['global_tag'])


    def test003w(self):
        """test003w: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', prep_id=testparams['prep_id'])

    def test003x(self):
        """test003x: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', cdate=int(time.time()))

    def test003y(self):
        """test003y: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', min_cdate=int(time.time()))

    def test003z(self):
        """test003z: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', max_cdate=int(time.time()))

    def test003za(self):
        """test003za: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', ldate=int(time.time()))

    def test003zb(self):
        """test003zb: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', min_ldate=int(time.time()))

    def test003zc(self):
        """test003zc: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', max_ldate=int(time.time()))

    def test003zd(self):
        """test003zd: web.DBSReaderModel.listDatasets: dataset, parent_dataset, release_version, pset_hash, app_name, output_module_label, global_tag"""
        api.list('datasets', dataset=testparams['dataset'],
                 parent_dataset='*',
                 release_version=testparams['release_version'],
                 pset_hash=testparams['pset_hash'],
                 app_name=testparams['app_name'],
                 output_module_label=testparams['output_module_label'],
                 global_tag=testparams['global_tag'])

    def test003ze(self):
        """test003ze: web.DBSReaderModel.listDatasets: dataset, parent_dataset, release_version, pset_hash, app_name, output_module_label, global_tag, detail"""
        api.list('datasets', dataset=testparams['dataset'],
                 parent_dataset='*',
                 release_version=testparams['release_version'],
                 pset_hash=testparams['pset_hash'],
                 app_name=testparams['app_name'],
                 output_module_label=testparams['output_module_label'],
                 global_tag=testparams['global_tag'],
                 detail=True)

    def test003zf(self):
        """test003zf: web.DBSReaderModel.listDatasets: dataset, pre_id, min_cdate, min_ldate, max_cdate, max_ldate"""
        api.list('datasets', dataset=testparams['dataset'],
                 prep_id=testparams['prep_id'],
                 min_cdate=0, min_ldate=0, max_cdate=int(time.time()), max_ldate=int(time.time()))

    def test003zg(self):
        """test003zg: web.DBSReaderModel.listDatasets: dataset, pre_id, cdate, ldate"""
        api.list('datasets', dataset=testparams['dataset'],
                 prep_id=testparams['prep_id'],
                 cdate=int(time.time()), ldate=int(time.time()))

    def test003zh(self):
        """test003zh: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', dataset=testparams['dataset'],
                 release_version=testparams['release_version']
                 )

    def test003zi(self):
        """test003zi: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', release_version=testparams['release_version'],
                 pset_hash=testparams['pset_hash'],
                 )

    def test003zj(self):
        """test003zj: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', app_name=testparams['app_name'],
                 output_module_label=testparams['output_module_label'])

    def test003zk(self):
        """test003zk: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', dataset=testparams['dataset'],
                 app_name=testparams['app_name'],
                 output_module_label=testparams['output_module_label'])

    def test003zl(self):
        """test003zl: web.DBSReaderModel.listDatasets: dataset, app_name, output_module_label, detail"""
        api.list('datasets', dataset=testparams['dataset'],
                 app_name=testparams['app_name'],
                 output_module_label=testparams['output_module_label'],
                 detail = True)

    def test003zj2(self):
        """test003zj: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', app_name=testparams['app_name'],
                 global_tag=testparams['global_tag'])

    def test003zk2(self):
        """test003zk: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', dataset=testparams['dataset'],
                 app_name=testparams['app_name'],
                 global_tag=testparams['global_tag'])

    def test003zl2(self):
        """test003zl: web.DBSReaderModel.listDatasets: dataset, app_name, global_tag, detail"""
        api.list('datasets', dataset=testparams['dataset'],
                 app_name=testparams['app_name'],
                 global_tag=testparams['global_tag'],
                 detail = True)


    def test003zm(self):
        """test003zm: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', processing_version=testparams['processing_version'])

    @checkException400
    def test003zn(self):
        """test003zn: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', processing_version=testparams['processing_version']+'*')

    def test003zo(self):
        """test003zo: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', acquisition_era_name=testparams['acquisition_era'])

    def test003zp(self):
        """test003zp: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', acquisition_era_name=testparams['acquisition_era']+'*')

    def test003zq(self):
        """test003zq: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', processed_ds_name=testparams['procdataset'])

    def test003zr(self):
        """test003zr: web.DBSReaderModel.listDatasets: basic test"""
        api.list('datasets', processed_ds_name=testparams['procdataset']+'*')

    def test003zs(self):
        """test003zs: web.DBSReaderModel.listDatasets: processing_version, acquisition_era, processed_ds_name"""
        api.list('datasets', processed_ds_name=testparams['procdataset'],
                 processing_version=testparams['processing_version'],
                 acquisition_era_name=testparams['acquisition_era'])

    def test003zt(self):
        """test003zt: web.DBSReaderModel.listDatasets: create_by"""
        api.list('datasets', create_by='giffels')

    def test003zu(self):
        """test003zu: web.DBSReaderModel.listDatasets: modified_by"""
        api.list('datasets', last_modified_by='giffels')

    def test003zv(self):
        """test003zv: web.DBSReaderModel.listDatasets: run and detail true"""
        api.list('datasets', dataset=testparams['dataset'], run_num=str(testparams['run_num']), detail=True)

    def test003zw(self):
        """test003zw: web.DBSReaderModel.listDatasets: dataset_id"""
	res=api.list('datasets', dataset=testparams['child_dataset'], detail=True)
        api.list('datasets', dataset_id=res[0]["dataset_id"])

    def test003zx(self):
        """test003zx: web.DBSReaderModel.listDatasets: dataset_id,detail"""
        res=api.list('datasets', dataset=testparams['dataset'], detail=True, dataset_access_type='*')
        api.list('datasets', dataset_id=res[0]["dataset_id"], detail=True, dataset_access_type='*')

    def test004a(self):
        """test004a: web.DBSReaderModel.listDatasetArray: basic test"""
        data = {'dataset':[testparams['dataset'], testparams['child_dataset']]}
        api.insert('datasetlist', data) ## it is a post request, therefore insert is used

    def test004b(self):
        """test004b: web.DBSReaderModel.listDatasetArray: basic test"""
	res1 = api.list('datasets', dataset=testparams['dataset'], detail=True, dataset_access_type='*')
        res2 = api.list('datasets', dataset=testparams['child_dataset'], detail=True)
        data = {'dataset_id':[res1[0]['dataset_id'], res2[0]['dataset_id']]}
        api.insert('datasetlist', data) ## it is a post request, therefore insert is used

    def test004c(self):
        """test004c: web.DBSReaderModel.listDatasetArray: basic test"""
        data = {'dataset':[testparams['dataset'], testparams['child_dataset']], 'detail':True}
        api.insert('datasetlist', data) ## it is a post request, therefore insert is used
    
    def test004d(self):
        """test004d: web.DBSReaderModel.listDatasetArray: basic test"""
        res1 = api.list('datasets', dataset=testparams['dataset'], detail=True, dataset_access_type='*')
        res2 = api.list('datasets', dataset=testparams['child_dataset'], detail=True)
        data = {'dataset_id':[res1[0]['dataset_id'], res2[0]['dataset_id']], 'detail':True}
        api.insert('datasetlist', data) ## it is a post request, therefore insert is used

    
    def test005a(self):
        """test005a: web.DBSReaderModel.listDataTiers: basic test"""
        api.list('datatiers')

    def test005b(self):
        """test005b: web.DBSReaderModel.listDataTiers: basic test"""
        api.list('datatiers', data_tier_name=testparams['tier'])

    def test005c(self):
        """test005c: web.DBSReaderModel.listDataTiers: basic test"""
        api.list('datatiers', data_tier_name=testparams['tier']+'*')

    @checkException400
    def test006a(self):
        """test006a: web.DBSReaderModel.listBlocks: basic negative test"""
        api.list('blocks', dataset='/*')

    def test006b(self):
        """test006b: web.DBSReaderModel.listBlocks: basic test"""
        api.list('blocks', dataset=testparams['dataset'])

    def test006c(self):
        """test006c: web.DBSReaderModel.listBlocks: basic test"""
        api.list('blocks', block_name=testparams['block'])

    @checkException400
    def test006d(self):
        """test006d: web.DBSReaderModel.listBlocks: basic negative test"""
        api.list('blocks', origin_site_name=testparams['site'])

    def test006e(self):
        """test006e: web.DBSReaderModel.listBlocks: basic test"""
        api.list('blocks', run_num=testparams['run_num'], block_name=testparams['block'])
    
    def test006ed(self):
        """test006ed: web.DBSReaderModel.listBlocks: basic test"""
        api.list('blocks', run_num=testparams['run_num'], block_name=testparams['block'], detail=True)    

    @checkException400
    def test006f(self):
        """test006f: web.DBSReaderModel.listBlocks: basic negative test"""
        api.list('blocks', block_name='/*')

    @checkException400
    def test006g(self):
        """test006g: web.DBSReaderModel.listBlocks: basic negative test"""
        api.list('blocks', origin_site_name='*')

    def test006h(self):
        """test006h: web.DBSReaderModel.listBlocks: basic test"""
        api.list('blocks', dataset=testparams['dataset'],
                 origin_site_name=testparams['site'])

    def test006i(self):
        """test006i: web.DBSReaderModel.listBlocks: basic test"""
        api.list('blocks', dataset=testparams['dataset'],
                 origin_site_name=testparams['site'],
                 detail=True)

    def test006j(self):
        """test006j: web.DBSReaderModel.listBlocks: basic test"""
        api.list('blocks', dataset=testparams['dataset'],
                 block_name=testparams['block'],
                 origin_site_name=testparams['site'])

    @checkException400
    def test006k(self):
        """test006k: web.DBSReaderModel.listBlocks: Must raise an exception if no parameter is passed."""
        api.list('blocks')

    @checkException400
    def test006l(self):
        """test006l: web.DBSReaderModel.listBlocks: input validation test wildcard data_tier_name."""
        api.list('blocks', data_tier_name='*')

    @checkException400
    def test006m(self):
        """test006m: web.DBSReaderModel.listBlocks: input validation test only data_tier_name."""
        api.list('blocks', data_tier_name=testparams['tier'])

    @checkException400
    def test006n(self):
        """test006n: web.DBSReaderModel.listBlocks: input validation test only data_tier_name and min_cdate."""
        api.list('blocks', data_tier_name=testparams['tier'], min_cdate=self._current_unixtime-30*24*3600)

    @checkException400
    def test006o(self):
        """test006o: web.DBSReaderModel.listBlocks: input validation test only data_tier_name and max_cdate."""
        api.list('blocks', data_tier_name=testparams['tier'], max_cdate=self._current_unixtime)

    @checkException400
    def test006p(self):
        """test006p: web.DBSReaderModel.listBlocks: input validation test exceeded time range"""
        api.list('blocks', data_tier_name=testparams['tier'], min_cdate=self._current_unixtime-35*24*3600,
                 max_cdate=self._current_unixtime)

    def test006q(self):
        """test006q: web.DBSReaderModel.listBlocks: data_tier_name, min_cdate, max_cdate"""
        api.list('blocks', data_tier_name=testparams['tier'], min_cdate=self._current_unixtime-30*24*3600,
                 max_cdate=self._current_unixtime)

    def test006r(self):
        """test006r: web.DBSReaderModel.listBlocks: dataset, runlist"""
        api.list('blocks', run_num=[testparams['run_num'], testparams['run_num']], dataset=testparams['dataset'])

    def test006rd(self):
        """test006rd: web.DBSReaderModel.listBlocks: dataset, runlist and details"""
        api.list('blocks', run_num=[testparams['run_num'], testparams['run_num']], dataset=testparams['dataset'],
                 detail=True)

    @checkException400
    def test006s(self):
        """test006s: web.DBSReaderModel.listBlockOrigin: basic test """
        api.list('blockorigin', origin_site_name=testparams['site'])

    def test006t(self):
        """test006t: web.DBSReaderModel.listBlockOrigin: basic test """
        api.list('blockorigin', origin_site_name=testparams['site'], dataset=testparams['dataset'])

    @checkException400
    def test006u(self):
        """test006u: web.DBSReaderModel.listBlockOrigin: Must raise an exception if no parameter is passed. """
        api.list('blockorigin')

    def test006ua(self):
        """test006ua: web.DBSReaderModel.listBlockOrigin: basic test """
        api.list('blockorigin', origin_site_name=testparams['site'], block_name=testparams['block'])

    def test006ub(self):
        """test006ub: web.DBSReaderModel.listBlockOrigin: basic test """
        api.list('blockorigin', block_name=testparams['block'])

    def test006uc(self):
        """test006uc: web.DBSReaderModel.listBlockOrigin: basic test """
        api.list('blockorigin', dataset=testparams['dataset'])

    def test006v(self):
        """test006v: web.DBSReaderModel.listBlocks: basic test"""
        api.list('blocks', run_num=testparams['run_num'], block_name=testparams['block'])

    def test006vd(self):
        """test006vd: web.DBSReaderModel.listBlocks: basic test"""
        api.list('blocks', run_num=testparams['run_num'], block_name=testparams['block'], detail=True)

    @checkException400
    def test007a(self):
        """test007a: web.DBSReaderModel.listFiles: basic negative test"""
        api.list('files', dataset='/*')

    def test007b(self):
        """test007b: web.DBSReaderModel.listFiles: basic test"""
        api.list('files', dataset=testparams['dataset'], run_num='1-%s' % (testparams['run_num']))

    def test007b1(self):
        """test007b1: web.DBSReaderModel.listFiles: basic test"""
        api.list('files', dataset=testparams['dataset'], run_num='1-%s' % (testparams['run_num']), detail=True)

    def test007b2(self):
        """test007b2: web.DBSReaderModel.listFiles: basic test"""
        api.list('files', dataset=testparams['dataset'], run_num=['1-%s' % (testparams['run_num']), 100, 200, 300, '500-600'] )

    def test007b3(self):
        """test007b3: web.DBSReaderModel.listFiles: basic test"""
        api.list('files', dataset=testparams['dataset'], run_num=['1-%s' % (testparams['run_num']), 100, 200, 300, '500-600'], detail=True)

    def test007c1(self):
        """test007c1: web.DBSReaderModel.listFiles: basic test"""
        api.list('files', dataset=testparams['dataset'])

    def test007c2(self):
        """test007c2: web.DBSReaderModel.listFiles: basic test"""
        api.list('files', dataset=testparams['dataset'], validFileOnly=1)

    def test007c3(self):
        """test007c3: web.DBSReaderModel.listFiles: basic test"""
        api.list('files', dataset=testparams['dataset'], validFileOnly=0)

    def test007d1(self):
        """test007d1: web.DBSReaderModel.listFiles: with dataset, lumi list"""
        api.list('files', dataset=testparams['dataset'], lumi_list="[27414, 26422, 29838]",
                 run_num=testparams['run_num'])

    def test007d2(self):
        """test007d2: web.DBSReaderModel.listFiles: with dataset, lumi list, validFileOnly"""
        api.list('files', dataset=testparams['dataset'], lumi_list="[27414, 26422, 29838]",
                 run_num=testparams['run_num'], validFileOnly=1)

    def test007d3(self):
        """test007d3: web.DBSReaderModel.listFiles: with dataset, lumi list, validFileOnly"""
        api.list('files', dataset=testparams['dataset'], lumi_list="[27414, 26422, 29838]",
                 run_num=testparams['run_num'], validFileOnly=0)
    
    def test007dd(self):
        """test007dd: web.DBSReaderModel.listFiles: with dataset, lumi list"""
        api.list('files', dataset=testparams['dataset'], lumi_list="[27414, 26422, 29838]",
                 run_num=testparams['run_num'], detail=True)

    def test007d4(self):
        """test007d4: web.DBSReaderModel.listFiles: with dataset, lumi list"""
        api.list('files', dataset=testparams['dataset'], lumi_list="[27414, 26422, 29838]",
                 run_num=testparams['run_num'])

    def test007d4a(self):
        """test007d4a: web.DBSReaderModel.listFiles: with dataset, lumi list"""
        api.list('files', dataset=testparams['dataset'], lumi_list=[27414, 26422, 29838],
                 run_num=testparams['run_num'])	

    def test007d5(self):
        """test007d5: web.DBSReaderModel.listFiles: with dataset, lumi list, validFileOnly"""
        api.list('files', dataset=testparams['dataset'], lumi_list="[27414, 26422, 29838]",
                 run_num=testparams['run_num'], validFileOnly=1)

    def test007d6(self):
        """test007d6: web.DBSReaderModel.listFiles: with dataset, lumi list, validFileOnly"""
        api.list('files', dataset=testparams['dataset'], lumi_list="[27414, 26422, 29838]",
                 run_num=testparams['run_num'], validFileOnly=0)

    def test007d7(self):
        """test007d7: web.DBSReaderModel.listFiles: with dataset, lumi list"""
        api.list('files', dataset=testparams['dataset'], lumi_list=[27414, 26422, 29838],
			run_num=testparams['run_num'], detail=True)

    def test007d8(self):
        """test007d8: web.DBSReaderModel.listFiles: with dataset, lumi list"""
        api.list('files', dataset=testparams['dataset'], lumi_list="[[27414, 26422], [29838,30000]]",
                 run_num=testparams['run_num'], detail=True)

    def test007d9(self):
        """test007d9: web.DBSReaderModel.listFiles: with dataset, lumi list"""
        api.list('files', dataset=testparams['dataset'], lumi_list="[[27414, 26422], [29838,30000]]",
                 run_num=testparams['run_num'])

    def test007d10(self):
        """test007d10: web.DBSReaderModel.listFiles: with dataset, lumi list"""
        api.list('files', dataset=testparams['dataset'], lumi_list=[[27414, 26422], [29838, 30000]],
                 run_num=testparams['run_num']) 

    def test007d11(self):
        """test007d12: web.DBSReaderModel.listFiles: with dataset, lumi list, validFileOnly"""
        api.list('files', dataset=testparams['dataset'], lumi_list="[[27414, 26422], [29838, 30000]]",
                 run_num=testparams['run_num'], validFileOnly=1)

    def test007d12(self):
        """test007d12: web.DBSReaderModel.listFiles: with dataset, lumi list, validFileOnly"""
        api.list('files', dataset=testparams['dataset'], lumi_list="[[27414, 26422], [29838,30000]]",
                 run_num=testparams['run_num'], validFileOnly=0)

    def test007d13(self):
        """test007d13: web.DBSReaderModel.listFiles: with dataset, lumi list"""
        api.list('files', dataset=testparams['dataset'], lumi_list=[[27414, 26422], [29838, 30000]],
                        run_num=testparams['run_num'], detail=True)

    def test007e(self):
        """test007e: web.DBSReaderModel.listFiles: with dataset and lumi intervals"""
        api.list('files', dataset=testparams['dataset'], lumi_list=[[1, 100]], run_num=testparams['run_num'])

    def test007ed(self):
        """test007ed: web.DBSReaderModel.listFiles: with dataset and lumi intervals"""
        api.list('files', dataset=testparams['dataset'], lumi_list=[[1, 100]], run_num=testparams['run_num'], detail=True)

    @checkException400
    def test007f(self):
        """test007f: web.DBSReaderModel.listFiles: basic negative test"""
        api.list('files', dataset=testparams['dataset']+'*')

    def test007g(self):
        """test007g: web.DBSReaderModel.listFiles: basic test"""
        api.list('files', dataset=testparams['dataset'], origin_site_name=testparams['site'])

    @checkException400
    def test007h(self):
        """test007h: web.DBSReaderModel.listFiles: basic negative test """
        api.list('files', block_name='/*')

    def test007i1(self):
        """test007i1: web.DBSReaderModel.listFiles: basic test """
        api.list('files', block_name=testparams['block'])

    def test007i2(self):
        """test007i2: web.DBSReaderModel.listFiles: basic test """
        api.list('files', block_name=testparams['block'], validFileOnly=1)

    def test007i3(self):
        """test007i3: web.DBSReaderModel.listFiles: basic test """
        api.list('files', block_name=testparams['block'], validFileOnly=0)
    
    def test007ia1(self):
        """test007ia1: web.DBSReaderModel.listFiles: detail test """
        api.list('files', block_name=testparams['block'], detail=True)

    def test007ia2(self):
        """test007ia2: web.DBSReaderModel.listFiles: detail test """
        api.list('files', block_name=testparams['block'], detail=True, validFileOnly=1)

    def test007ia3(self):
        """test007ia3: web.DBSReaderModel.listFiles: detail test """
        api.list('files', block_name=testparams['block'], detail=True, validFileOnly=0)

    @checkException400
    def test007j(self):
        """test007j: web.DBSReaderModel.listFiles: basic negative test """
        api.list('files', logical_file_name='/*')

    def test007k1(self):
        """test007k1: web.DBSReaderModel.listFiles: basic test """
        lfn= testparams['files'][1]
        api.list('files', logical_file_name=lfn)

    def test007k2(self):
        """test007k2: web.DBSReaderModel.listFiles: basic test """
        lfn= testparams['files'][1]
        api.list('files', logical_file_name=lfn, validFileOnly=1)

    def test007k3(self):
        """test007k3: web.DBSReaderModel.listFiles: basic test """
        lfn= testparams['files'][1]
        api.list('files', logical_file_name=lfn, validFileOnly=0)

    def test007k4(self):
        """test007k4: web.DBSReaderModel.listFileArray: basic test """
        lfn= [testparams['files'][1], testparams['files'][2], testparams['files'][3]]
	data ={"logical_file_name":lfn}
        api.insert('fileArray', data)

    def test007k5(self):
        """test007k5: web.DBSReaderModel.listFileArray: basic test """
        lfn= [testparams['files'][1], testparams['files'][2], testparams['files'][3]]
        data ={"logical_file_name":lfn, "detail": 1}
	#import pdb
	#pdb.set_trace()
        api.insert('fileArray', data)

    def test007k6(self):
        """test007k6: web.DBSReaderModel.listFileArray: basic test """
        lfn= [testparams['files'][1], testparams['files'][2], testparams['files'][3]]
        data ={"logical_file_name":lfn, "detail": 1, "validFileOnly":1}
        api.insert('fileArray', data)

    def test007k7(self):
        """test007k7: web.DBSReaderModel.listFileArray: basic test """
        lfn= [testparams['files'][1], testparams['files'][2], testparams['files'][3]]
        data ={"logical_file_name":lfn, "detail": 0, "validFileOnly":1}
        api.insert('fileArray', data)

    def test007k8(self):
        """test007k8: web.DBSReaderModel.listFileArray: basic test """
        lfn= [testparams['files'][1], testparams['files'][2], testparams['files'][3]]
        data ={"logical_file_name":lfn, "detail": 0, "validFileOnly":1, "run_num":testparams['run_num'] }
        api.insert('fileArray', data)

    def test007k9(self):
        """test007k9: web.DBSReaderModel.listFileArray: basic test """
        lfn= [testparams['files'][1], testparams['files'][2], testparams['files'][3]]
        data ={"logical_file_name":lfn, "detail": 1, "validFileOnly":1, "run_num":testparams['run_num'] }
        api.insert('fileArray', data)

    @checkException400	
    def test007k10(self):
        """test007k10: web.DBSReaderModel.listFileArray: basic test """
        lfn= [testparams['files'][1], testparams['files'][2], testparams['files'][3]]
        data ={"logical_file_name":lfn, "detail": 1, "validFileOnly":1, "run_num":[testparams['run_num'], testparams['run_num']] }
        api.insert('fileArray', data)

    def test007k11(self):
        """test007k11: web.DBSReaderModel.listFileArray: basic test """
        lfn= [testparams['files'][1], testparams['files'][2], testparams['files'][3]]
        data ={"logical_file_name":lfn, "detail": 1, "validFileOnly":1, "run_num":testparams['run_num'] , 
	       "output_module_label":testparams['output_module_label'], "release_version":testparams['release_version']	}
	api.insert('fileArray', data)

    def test007K12(self):
        """test007k12: web.DBSReaderModel.listFileArray: with dataset, lumi list that has to be a list"""
        data = {'dataset':testparams['dataset'], 'lumi_list': [27414, 26422, 29838 ],
                 'run_num':testparams['run_num']}
	api.insert('fileArray', data)

    def test007k13(self):
        """test007k13: web.DBSReaderModel.listFileArray: with dataset, lumi list that has to be a list, validFileOnly"""
        data = {'dataset' : testparams['dataset'], 'lumi_list': [27414, 26422, 29838],
                 'run_num': testparams['run_num'], 'validFileOnly':1}
	api.insert('fileArray', data)

    def test007k14(self):
        """test007k14: web.DBSReaderModel.listFileArray: with dataset, lumi list that has to be a list, validFileOnly"""
        data = {'dataset': testparams['dataset'], 'lumi_list': [27414, 26422, 29838],
                 'run_num' : testparams['run_num'], 'validFileOnly':0}
	api.insert('fileArray', data)	

    def test007k15(self):
        """test007k15: web.DBSReaderModel.listFileArray: with dataset, lumi list that has to be a list"""
        data = { 'dataset':testparams['dataset'], 'lumi_list': [27414, 26422, 29838],
		'run_num' : testparams['run_num'], 'detail':1}
	api.insert('fileArray', data)

    def test007K16(self):
        """test007k16: web.DBSReaderModel.listFileArray: with dataset, lumi list that has to be a list"""
        data = {'dataset':testparams['dataset'], 'lumi_list': [[27414, 27422], [39999, 40000] ],
                 'run_num':testparams['run_num']}
        api.insert('fileArray', data)

    def test007k17(self):
        """test007k17: web.DBSReaderModel.listFileArray: with dataset, lumi list that has to be a list, validFileOnly"""
        data = {'dataset' : testparams['dataset'], 'lumi_list': [[27414, 26422], [29838, 30000]],
                 'run_num': testparams['run_num'], 'validFileOnly':1}
        api.insert('fileArray', data)

    def test007k18(self):
        """test007k18: web.DBSReaderModel.listFileArray: with dataset, lumi list that has to be a list, validFileOnly"""
        data = {'dataset': testparams['dataset'], 'lumi_list': [[27414, 26422], [29838, 30000]],
                 'run_num' : testparams['run_num'], 'validFileOnly':0}
        api.insert('fileArray', data)   

    def test007k19(sel):
        """test007k19: web.DBSReaderModel.listFileArray: with dataset, lumi list that has to be a list"""
        data = { 'dataset':testparams['dataset'], 'lumi_list': [[27414, 26422], [29838, 30000]],
                'run_num' : testparams['run_num'], 'detail':1}
        api.insert('fileArray', data)

    def test007k20(self):
        """test007k20: web.DBSReaderModel.listFileArray: basic test"""
        data = {'dataset':testparams['dataset'], 'run_num': ['1-%s' % (testparams['run_num']), 100, 200, 300, '500-600'] }
	api.insert('fileArray', data)

    def test007k17(self):
        """test007k17: web.DBSReaderModel.listFileArray: basic test"""
        data = {'dataset':testparams['dataset'], 'run_num' : ['1-%s' % (testparams['run_num']), 100, 200, 300, '500-600'] , 'detail':True}
	api.insert('fileArray', data)

    def test007ka1(self):
        """test007ka1: web.DBSReaderModel.listFiles: detail test """
        lfn= testparams['files'][1]
        api.list('files', logical_file_name=lfn, detail=True, validFileOnly=1)

    def test007ka2(self):
        """test007ka2: web.DBSReaderModel.listFiles: detail test """
        lfn= testparams['files'][1]
        api.list('files', logical_file_name=lfn, detail=True)

    def test007ka3(self):
        """test007ka3: web.DBSReaderModel.listFiles: detail test """
        lfn= testparams['files'][1]
        api.list('files', logical_file_name=lfn, detail=True, validFileOnly=0)

    @checkException400
    def test007l(self):
        """test007l: web.DBSReaderModel.listFiles: Must raise an exception if no parameter is passed. """
        api.list('files')

    @checkException400
    def test007m(self):
        """test007m: web.DBSReaderModel.listFiles: basic negative test"""
        api.list('files', release_version=testparams['release_version'])

    @checkException400
    def test007n(self):
        """test007n: web.DBSReaderModel.listFiles: basic negative test"""
        api.list('files', pset_hash=testparams['pset_hash'])

    @checkException400
    def test007o(self):
        """test007o: web.DBSReaderModel.listFiles: basic negative test"""
        api.list('files', app_name=testparams['app_name'])

    @checkException400
    def test007p(self):
        """test007p: web.DBSReaderModel.listFiles: basic negative test"""
        api.list('files', output_module_label=testparams['output_module_label'])

    def test007q(self):
        """test007q: web.DBSReaderModel.listFiles: basic test"""
        lfn= testparams['files'][1]
        api.list('files', logical_file_name=lfn, release_version=testparams['release_version'])

    def test007r(self):
        """test007r: web.DBSReaderModel.listFiles: basic test"""
        lfn= testparams['files'][1]
        api.list('files', logical_file_name=lfn, pset_hash=testparams['pset_hash'])

    def test007s(self):
        """test007s: web.DBSReaderModel.listFiles: basic test"""
        lfn= testparams['files'][1]
        api.list('files', logical_file_name=lfn, app_name=testparams['app_name'])

    def test007t(self):
        """test007t: web.DBSReaderModel.listFiles: basic test"""
        lfn= testparams['files'][1]
        api.list('files', logical_file_name=lfn, output_module_label=testparams['output_module_label'])

    def test007u(self):
        """test007u: web.DBSReaderModel.listFiles: basic test"""
        lfn= testparams['files'][1]
        api.list('files', logical_file_name=lfn, output_module_label=testparams['output_module_label'],
                 app_name=testparams['app_name'], pset_hash=testparams['pset_hash'],
                 release_version=testparams['release_version'], detail=True)
    def test007v1(self):
        """test007v1: web.DBSReaderModel.listFiles: basic test """
        api.list('files', block_name=testparams['block'], detail=True)

    def test007v2(self):
        """test007v2: web.DBSReaderModel.listFiles: basic test """
        api.list('files', block_name=testparams['block'], detail=True, validFileOnly=1)

    def test007v3(self):
        """test007v3: web.DBSReaderModel.listFiles: basic test """
	lfn = testparams['files'][1]
        api.list('files', block_name=testparams['block'], detail=True, validFileOnly=1, logical_file_name=lfn)

    def test007v4(self):
        """test007v4: web.DBSReaderModel.listFiles: basic test """
        lfn = testparams['files'][1]+"*"
        api.list('files', block_name=testparams['block'], detail=True, validFileOnly=1, logical_file_name=lfn)

    def test007v5(self):
        """test007v5: web.DBSReaderModel.listFiles: basic test """
        lfn = testparams['files'][1]
        api.list('files', block_name=testparams['block'], validFileOnly=1, logical_file_name=lfn)

    def test007v6(self):
        """test007v6: web.DBSReaderModel.listFiles: basic test """
        lfn = testparams['files'][1]+"*"
        api.list('files', block_name=testparams['block'], validFileOnly=1, logical_file_name=lfn)

    def test007w1(self):
        """test007w1: web.DBSReaderModel.listFiles: with dataset, run_num and detail"""
        api.list('files', dataset=testparams['dataset'], run_num=testparams['run_num'], detail=True)

    def test007w2(self):
        """test007w2: web.DBSReaderModel.listFiles: with dataset, run_num and detail"""
        api.list('files', dataset=testparams['dataset'], run_num=testparams['run_num'], detail=True, validFileOnly=1)

    def test007w3(self):
        """test007w3: web.DBSReaderModel.listFiles: with dataset, run_num and detail"""
	lfn = testparams['files'][1]+"*"
        api.list('files', dataset=testparams['dataset'], logical_file_name=lfn, validFileOnly=1)

    def test007w4(self):
        """test007w4: web.DBSReaderModel.listFiles: with dataset, run_num and detail"""
	lfn = testparams['files'][1]+"*"
        api.list('files', dataset=testparams['dataset'], logical_file_name=lfn, detail=True, validFileOnly=1)


    def test008a(self):
        """test008a: web.DBSReaderModel.listDatasetParents: basic test """
        api.list('datasetparents', dataset="/*")

    def test008b(self):
        """test008b: web.DBSReaderModel.listDatasetParents: basic test """
        api.list('datasetparents', dataset=testparams['dataset'])

    def test008c(self):
        """test008c: web.DBSReaderModel.listDatasetParents: basic test """
        api.list('datasetparents', dataset=testparams['dataset']+'*')

    @checkException400
    def test008d(self):
        """test008d: web.DBSReaderModel.listDatasetParents: must raise an exception if no parameter is passed """
        api.list('datasetparents')

    def test009a(self):
        """test009a: web.DBSReaderModel.listOutputConfigs: basic test """
        api.list('outputconfigs')

    def test009b(self):
        """test009b: web.DBSReaderModel.listOutputConfigs: basic test """
        api.list('outputconfigs', dataset="/*")

    def test009c(self):
        """test009c: web.DBSReaderModel.listOutputConfigs: basic test """
        api.list('outputconfigs', dataset=testparams['dataset'])

    def test009d(self):
        """test009d: web.DBSReaderModel.listOutputConfigs: basic test """
        api.list('outputconfigs', dataset=testparams['dataset']+"*")

    @checkException400
    def test009e(self):
        """test009e: web.DBSReaderModel.listOutputConfigs: basic test """
        api.list('outputconfigs', logical_file_name="/*")

    def test009f(self):
        """test009f: web.DBSReaderModel.listOutputConfigs: basic test """
        #need to be updated with LFN
        lfn=testparams['files'][1]
        api.list('outputconfigs', logical_file_name=lfn)

    @checkException400
    def test009g(self):
        """test009g: web.DBSReaderModel.listOutputConfigs: basic test """
        #need to be updated with LFN
        lfn=testparams['files'][1]
        api.list('outputconfigs', logical_file_name=lfn+"*")

    def test009h(self):
        """test009h: web.DBSReaderModel.listOutputConfigs: basic test """
        api.list('outputconfigs', release_version="*")

    def test009i(self):
        """test009i: web.DBSReaderModel.listOutputConfigs: basic test """
        api.list('outputconfigs', release_version=testparams['release_version'])

    def test009j(self):
        """test009j: web.DBSReaderModel.listOutputConfigs: basic test """
        api.list('outputconfigs', release_version=testparams['release_version']+'*')

    def test009k(self):
        """test009k: web.DBSReaderModel.listOutputConfigs: basic test """
        api.list('outputconfigs', pset_hash="*")

    def test009l(self):
        """test009l: web.DBSReaderModel.listOutputConfigs: basic test """
        api.list('outputconfigs', pset_hash=testparams['pset_hash'])

    def test009m(self):
        """test009m: web.DBSReaderModel.listOutputConfigs: basic test """
        api.list('outputconfigs', app_name="*")

    def test009n(self):
        """test009n: web.DBSReaderModel.listOutputConfigs: basic test """
        api.list('outputconfigs', app_name=testparams['app_name'])

    def test009o(self):
        """test009o: web.DBSReaderModel.listOutputConfigs: basic test """
        api.list('outputconfigs', app_name=testparams['app_name']+"*")

    def test009p(self):
        """test009p: web.DBSReaderModel.listOutputConfigs: basic test """
        api.list('outputconfigs', output_module_label="*")

    def test009q(self):
        """test009q: web.DBSReaderModel.listOutputConfigs: basic test """
        api.list('outputconfigs', output_module_label=testparams['output_module_label'])

    def test009r(self):
        """test009r: web.DBSReaderModel.listOutputConfigs: basic test """
        api.list('outputconfigs', output_module_label=testparams['output_module_label']+'*')

    def test009s(self):
        """test009s: web.DBSReaderModel.listOutputConfigs: basic test """
        api.list('outputconfigs', dataset=testparams['dataset'],
                 release_version=testparams['release_version'],
                 pset_hash=testparams['pset_hash'],
                 app_name=testparams['app_name'],
                 output_module_label=testparams['output_module_label'],
                 global_tag=testparams['global_tag'])

    def test009t(self):
        """test009t: web.DBSReaderModel.listOutputConfigs: basic test """
        api.list('outputconfigs', dataset=testparams['dataset'],
                 release_version=testparams['release_version'],
                 output_module_label=testparams['output_module_label'])

    @checkException400
    def test009u(self):
        """test009u: web.DBSReaderModel.listOutputConfigs: basic test """
        api.list('outputconfigs', logical_file_name="*",
                 app_name=testparams['app_name'],
                 output_module_label=testparams['output_module_label'])

    def test009v(self):
        """test009v: web.DBSReaderModel.listOutputConfigs: basic test """
        api.list('outputconfigs', dataset=testparams['dataset'],
                 release_version=testparams['release_version'],
                 global_tag=testparams['global_tag']
                 )

    def test009w(self):
        """test009w: web.DBSReaderModel.listOutputConfigs: basic test """
        api.list('outputconfigs', global_tag=testparams['global_tag'])

    def test009x(self):
        """test009x: web.DBSReaderModel.listOutputConfigs: basic test """
        api.list('outputconfigs', block_id=145635)

    def test010a(self):
        """test010a: web.DBSReaderModel.listFileParents: basic test """
        api.list('fileparents', logical_file_name="*")

    def test010b(self):
        """test010b: web.DBSReaderModel.listFileParents: basic test """
        api.list('fileparents', logical_file_name="/ABC*")

    @checkException400
    def test010c(self):
        """test010c: web.DBSReaderModel.listFileParents: must raise an exception if no parameter is passed """
        api.list('fileparents')

    def test010d(self):
        """test010d: web.DBSReaderModel.listFileParents: basic lfn list test"""
        api.list('fileparents', logical_file_name=testparams['parent_files'])

    @checkException400
    def test010e(self):
        """test010e: web.DBSReaderModel.listFileLumis: basic test """
        api.list('filelumis', logical_file_name="*")

    def test010f(self):
        """test010f: web.DBSReaderModel.listFileLumis: basic test """
        #need to update LFN
        lfn=testparams['files'][1]
        api.list('filelumis', logical_file_name=lfn)

    @checkException400
    def test010g(self):
        """test010g: web.DBSReaderModel.listFileLumis: basic test """
        api.list('filelumis', block_name="/*")

    def test010h(self):
        """test010h: web.DBSReaderModel.listFileLumis: basic test """
        api.list('filelumis', block_name=testparams['block'])

    @checkException400
    def test010i(self):
        """test010i: web.DBSReaderModel.listFileLumis: basic test """
        api.list('filelumis', block_name=testparams['block']+'*')

    @checkException400
    def test010j(self):
        """test010j: web.DBSReaderModel.listFileLumis: must raise an exception if no parameter is passed """
        api.list('filelumis')

    def test010k(self):
        """test010k: web.DBSReaderModel.listFileLumis: basic test """
        lfn = testparams['files'][1]
        api.list('filelumis', logical_file_name=lfn)

    def test010l(self):
        """test010l: web.DBSReaderModel.listFileLumis: basic test """
        lfn = testparams['files'][1]
        api.list('filelumis', logical_file_name=lfn, run_num=testparams['run_num'])

    def test010m(self):
        """test010lm: web.DBSReaderModel.listFileLumis: basic test """
        lfn = testparams['files'][1]
        api.list('filelumis', logical_file_name=lfn, run_num=testparams['run_num'], validFileOnly=1)

    def test010n(self):
        """test010n: web.DBSReaderModel.listFileLumis: basic test """
        api.list('filelumis', block_name=testparams['block'], validFileOnly=1)


    def test010o(self):
        """test010lo: web.DBSReaderModel.listFileLumis: basic test """
        lfn = testparams['files'][1]
        api.list('filelumis', logical_file_name=lfn, validFileOnly=1)

    def test010p(self):
        """test010lp: web.DBSReaderModel.listFileLumis: basic test """
        lfn = testparams['files'][1]
        api.list('filelumis',  block_name=testparams['block'], run_num=testparams['run_num'], validFileOnly=1)

    def test010q(self):
        """test010q: web.DBSReaderModel.listFileLumis: basic test """
        api.list('filelumis', block_name=testparams['block'], validFileOnly=1)


    def test010r(self):
        """test010lr: web.DBSReaderModel.listFileLumis: basic test """
        lfn = testparams['files'][1]
        api.list('filelumis', logical_file_name=lfn, validFileOnly="1")

    @checkException400
    def test010s(self):
        """test010s: web.DBSReaderModel.listFileLumis: basic test """
        lfn = testparams['files'][1]
        api.list('filelumis',  block_name=testparams['block'], run_num=testparams['run_num'], validFileOnly="*")

    def test010t(self):
        """test010lt: web.DBSReaderModel.listFileLumiArray: basic test """
        lfn1 = testparams['files'][1]
	lfn2 = testparams['files'][2]
	data = {"logical_file_name":[lfn1, lfn2], "validFileOnly":1}
        api.insert('filelumis', data)

    def test010u(self):
        """test010lu: web.DBSReaderModel.listFileLumiArray: basic test """
        lfn1 = testparams['files'][1]
	lfn2 = testparams['files'][2]
	data = {"logical_file_name": [lfn1, lfn2], "validFileOnly": 0}
        api.insert('filelumis', data)

    def test010w(self):
        """test010lw: web.DBSReaderModel.listFileLumiArray: basic test """
        lfn1 = testparams['files'][1]
        lfn2 = testparams['files'][2]
        data={"logical_file_name": [lfn1, lfn2], "run_num":'1-%s' %testparams['run_num'], "validFileOnly" :0}
        api.insert('filelumis', data)

    def test010x(self):
        """test010lx: web.DBSReaderModel.listFileLumiArray: basic test """
        lfn1 = testparams['files'][1]
	data={"logical_file_name": lfn1, "run_num":testparams['run_num'], "validFileOnly" :1}
        api.insert('filelumis', data)

    def test010y(self):
        """test010ly: web.DBSReaderModel.listFileLumiArray: basic test """
        lfn1 = testparams['files'][1]
	data={"logical_file_name": lfn1, "run_num": testparams['run_num'], "validFileOnly" :0}
        api.insert('filelumis', data)

    def test010z(self):
        """test010lz: web.DBSReaderModel.listFileLumiArray: basic test """
        lfn1 = testparams['files'][1]
	data={"logical_file_name": lfn1, "run_num": testparams['run_num'], "validFileOnly" :1}
        api.insert('filelumis', data)

    def test010z0(self):
        """test010lz0: web.DBSReaderModel.listFileLumiArray: basic test """
        lfn1 = testparams['files'][1]
	data={"logical_file_name": lfn1, "run_num": testparams['run_num'], "validFileOnly" :0}
        api.insert('filelumis', data)

    def test011a(self):
        """test011a: web.DBSReaderModel.listFile with maxrun, minrun: basic """
        api.list('files', run_num='%s-%s' % (testparams['run_num'], testparams['run_num']-1000),
                 dataset=testparams['dataset'])

    def test011b(self):
        """test011b: web.DBSReaderModel.listFile with maxrun, minrun, dataset and detail"""
        api.list('files', run_num='%s-%s' % (testparams['run_num'], testparams['run_num']-1000),
                 dataset=testparams['dataset'], detail = True)

    def test011c(self):
        """test011c: web.DBSReaderModel.listFile with maxrun, minrun:basic """
        api.list('files', run_num='%s-%s' % (testparams['run_num'], testparams['run_num']-1000),
                 block_name=testparams['block'])

    def test011d(self):
        """test011d: web.DBSReaderModel.listFile with maxrun, minrun, block_name and detail"""
        api.list('files', run_num='%s-%s' % (testparams['run_num'], testparams['run_num']-1000),
                 block_name=testparams['block'], detail = True)

    def test011e(self):
        """test011e: web.DBSReaderModel.listFile with original site: basic """
        api.list('files', origin_site_name=testparams['site'], dataset=testparams['dataset'])

    def test011f(self):
        """test011f: web.DBSReaderModel.listFile with original site, dataset and detail """
        api.list('files', origin_site_name=testparams['site'], dataset=testparams['dataset'], detail=True)

    def test011g(self):
        """test011g: web.DBSReaderModel.listFile with original site and dataset"""
        api.list('files', origin_site_name=testparams['site'], block_name=testparams['block'])

    def test011h(self):
        """test011h: web.DBSReaderModel.listFile with original site, block and detail"""
        api.list('files', origin_site_name=testparams['site'], block_name=testparams['block'], detail=True)

    def test011i(self):
        """test011i: web.DBSReaderModel.listFile with config info and block"""
        api.list('files',  block_name=testparams['block'],
                 release_version=testparams['release_version'], pset_hash=testparams['pset_hash'],
                 app_name=testparams['app_name'], output_module_label=testparams['output_module_label'])

    def test011j(self):
        """test011j: web.DBSReaderModel.listFile with config info, block and detail """
        api.list('files',  block_name=testparams['block'],
                 release_version=testparams['release_version'], pset_hash=testparams['pset_hash'],
                 app_name=testparams['app_name'], output_module_label=testparams['output_module_label'], detail=True)

    def test011k(self):
        """test011k: web.DBSReaderModel.listFile with config info and dataset"""
        api.list('files',  dataset=testparams['dataset'],
                 release_version=testparams['release_version'], pset_hash=testparams['pset_hash'],
                 app_name=testparams['app_name'], output_module_label=testparams['output_module_label'])

    def test011l(self):
        """test011l: web.DBSReaderModel.listFile with config info, dataset and detail"""
        api.list('files',  dataset=testparams['dataset'],
                 release_version=testparams['release_version'], pset_hash=testparams['pset_hash'],
                 app_name=testparams['app_name'], output_module_label=testparams['output_module_label'], detail=True)

    def test011m(self):
        """test011m: web.DBSReaderModel.listDatasets with processing era: basic """
        api.list('datasets', processing_version=testparams['processing_version'],)

    def test011n(self):
        """test011n: web.DBSReaderModel.listDatasets with acquisition era: basic """
        api.list('datasets', acquisition_era_name=testparams['acquisition_era'])

    def test012a(self):
        """test012a: web.DBSReaderModel.listDataTypes : basic """
        api.list('datatypes')

    def test012b(self):
        """test012b: web.DBSReaderModel.listDataTypes : basic """
        api.list('datatypes', dataset=testparams['dataset'] )

    def test012c(self):
        """test012c: web.DBSReaderModel.listDataTypes : basic """
        api.list('datatypes', dataset=testparams['dataset']+'*')

    def test012d(self):
        """test012d: web.DBSReaderModel.listDataTypes : basic """
        api.list('datatypes', datatype=testparams['primary_ds_type'] )

    def test012e(self):
        """test012e: web.DBSReaderModel.listDataTypes : basic """
        api.list('datatypes', datatype=testparams['primary_ds_type']+'*')

    def test013a(self):
        """test013a: web.DBSReaderModel.listRuns: basic"""
        api.list('runs', run_num='0-%s' % (testparams['run_num']))

    def test013b(self):
        """test013b: web.DBSReaderModel.listRuns: basic"""
        api.list('runs', run_num='0-%s' % (testparams['run_num']), logical_file_name=testparams['files'][0])

    def test013c(self):
        """test013c: web.DBSReaderModel.listRuns: basic"""
        api.list('runs', block_name=testparams['block'])

    def test013d(self):
        """Test013d: web.DBSReaderModel.listRuns: basic"""
        api.list('runs', dataset=testparams['dataset'])

    def test013e(self):
        """Test013e: web.DBSReaderModel.listRuns: basic"""
        lfn = testparams['files'][1]
        api.list('runs', logical_file_name=lfn)

    @checkException400
    def test013f(self):
        """Test013f: web.DBSReaderModel.listRuns: basic negative test"""
        api.list('runs', logical_file_name='/*')

    @checkException400
    def test013g(self):
        """Test013g: web.DBSReaderModel.listRuns: basic negative test"""
        api.list('runs', dataset='/*')

    @checkException400
    def test013h(self):
        """Test013h: web.DBSReaderModel.listRuns: basic negative test"""
        api.list('runs', block_name='/*')

    def test014a(self):
        """test014a:  web.DBSReaderModel.listDatasetParents: basic"""
        api.list('datasetparents', dataset=testparams['dataset'])

    def test015a(self):
        """test015a: web.DBSReaderModel.listDatasetChildren: basic"""
        api.list('datasetchildren', dataset=testparams['dataset'])

    @checkException400
    def test015b(self):
        """test015b: web.DBSReaderModel.listDatasetChildren: basic negative test"""
        api.list('datasetchildren')

    def test016a(self):
        """test016a: web.DBSReaderModel.listBlockParents: basic"""
        api.list('blockparents', block_name=testparams['block'])

    def test016b(self):
        """test016b: web.DBSReaderModel.listBlockParents: basic"""
        data = {'block_name':[testparams['block']]}
        api.insert('blockparents', data) #use insert since this is a POST call

    def test017a(self):
        """test106: web.DBSReaderModel.listBlockChildren: basic"""
        api.list('blockchildren', block_name=testparams['block'])

    def test018a(self):
        """test018a: web.DBSReaderModel.listFileParents: basic"""
        api.list('fileparents', logical_file_name=testparams['files'][0])

    def test019a(self):
        """test019a: web.DBSReaderModel.listFileChildren: basic logical_file_name test"""
        api.list('filechildren', logical_file_name=testparams['parent_files'][0])

    @checkException400
    def test019b(self):
        """test019b: web.DBSReaderModel.listFileChildren: basic negative test"""
        api.list('filechildren')

    def test019c(self):
        """test019c: web.DBSReaderModel.listFileChildren: basic block test"""
        api.list('filechildren', block_name=testparams['block'])

    def test019d(self):
        """test019d: web.DBSReaderModel.listFileChildren: basic lfn list test"""
        api.list('filechildren', logical_file_name=testparams['parent_files'])

    def test019e(self):
        """test019e: web.DBSReaderModel.listFileChildren: basic block_id test"""
        api.list('filechildren', block_id=123)

    @checkException400
    def test020a(self):
        """test020a: web.DBSReaderModel.listFileSummaries: basic negative test"""
        api.list('filesummaries')

    @checkException400
    def test020b(self):
        """test020b: web.DBSReaderModel.listFileSummaries: basic negative test"""
        api.list('filesummaries', block_name="/*")

    @checkException400
    def test020c(self):
        """test020c: web.DBSReaderModel.listFileSummaries: basic negative test"""
        api.list('filesummaries', dataset='/*')

    def test020d(self):
        """test020d: web.DBSReaderModel.listFileSummaries: basic test"""
        api.list('filesummaries', dataset=testparams['dataset'])

    def test020e(self):
        """test020e: web.DBSReaderModel.listFileSummaries: basic test"""
        api.list('filesummaries', block_name=testparams['block'])

    def test020f(self):
        """test020f: web.DBSReaderModel.listFileSummaries: basic test"""
        api.list('filesummaries', dataset=testparams['dataset'], block_name=testparams['block'],
                 run_num=testparams['run_num'])

    def test020g(self):
        """test020g: web.DBSReaderModel.listFileSummaries: basic test"""
        api.list('filesummaries', dataset=testparams['dataset'], validFileOnly="2")

    def test020h(self):
        """test020h: web.DBSReaderModel.listFileSummaries: basic test"""
        api.list('filesummaries', dataset=testparams['dataset'], validFileOnly=2)

    @checkException400
    def test020i(self):
        """test020i: web.DBSReaderModel.listFileSummaries: basic test"""
        api.list('filesummaries', dataset=testparams['dataset'], validFileOnly="*")

    def test021a(self):
        """test021a: web.DBSReaderModel.dumpBlock : basic """
        api.list('blockdump', block_name=testparams['block'])

    @checkException400
    def test021b(self):
        """test021b: web.DBSReaderModel.dumpBlock : basic negative test"""
        api.list('blockdump', block_name=testparams['block']+'*')

    @checkException400
    def test021c(self):
        """test021c: web.DBSReaderModel.dumpBlock : basic negative test"""
        api.list('blockdump')

    def test022a(self):
        """test022a: web.DBSReaderModel.listAcquisitionEras: basic test"""
        api.list('acquisitioneras')

    def test022b(self):
        """test022b: web.DBSReaderModel.listAcquisitionEras: basic test"""
        api.list('acquisitioneras', acquisition_era_name=testparams['acquisition_era'])

    def test022c(self):
        """test022c: web.DBSReaderModel.listAcquisitionEras: basic test"""
        api.list('acquisitioneras', acquisition_era_name=testparams['acquisition_era']+'*')

    def test023a(self):
        """test023a: web.DBSReaderModel.listReleaseVersions: basic test"""
        api.list('releaseversions')

    def test023b(self):
        """test023b: web.DBSReaderModel.listReleaseVersions: basic test"""
        api.list('releaseversions', release_version=testparams['release_version'])

    def test023c(self):
        """test023c: web.DBSReaderModel.listReleaseVersions: basic test"""
        api.list('releaseversions', release_version=testparams['release_version']+'*')

    def test023d(self):
        """test023d: web.DBSReaderModel.listReleaseVersions: basic test"""
        api.list('releaseversions', dataset=testparams['dataset'])

    @checkException400
    def test023e(self):
        """test023e: web.DBSReaderModel.listReleaseVersions: basic test"""
        api.list('releaseversions', dataset=testparams['dataset']+'*')

    def test023f(self):
        """test023f: web.DBSReaderModel.listReleaseVersions: basic test"""
        api.list('releaseversions', dataset=testparams['dataset'], release_version=testparams['release_version'])

    def test024a(self):
        """test024a: web.DBSReaderModel.listDatasetAccessTypes: basic test"""
        api.list('datasetaccesstypes')

    def test024b(self):
        """test024b: web.DBSReaderModel.listDatasetAccessTypes: basic test"""
        api.list('datasetaccesstypes', dataset_access_type='PRODUCTION')

    def test024c(self):
        """test024c: web.DBSReaderModel.listDatasetAccessTypes: basic test"""
        api.list('datasetaccesstypes', dataset_access_type='PROD'+'*')

    def test025a(self):
        """test025a: web.DBSReaderModel.listPhysicsGroups: basic test"""
        api.list('physicsgroups')

    def test025b(self):
        """test025b: web.DBSReaderModel.listPhysicsGroups: basic test"""
        api.list('physicsgroups', physics_group_name='SUSY')

    def test025c(self):
        """test025c: web.DBSReaderModel.listPhysicsGroups: basic test"""
        api.list('physicsgroups', physics_group_name='SUSY'+'*')

    @checkException400
    def test026a(self):
        """test026a: web.DBSReaderModel.listBlockSummaries: input validation test"""
        api.list('blocksummaries')

    @checkException400
    def test026b(self):
        """test026b: web.DBSReaderModel.listBlockSummaries: input validation test"""
        api.list('blocksummaries', block_name='/*/B/C#abcdef12345')

    @checkException400
    def test026c(self):
        """test026c: web.DBSReaderModel.listBlockSummaries: input validation test"""
        api.list('blocksummaries', dataset='/*/B/C')

    @checkException400
    def test026d(self):
        """test026d: web.DBSReaderModel.listBlockSummaries: input validation test"""
        api.list('blocksummaries', dataset=testparams['dataset'], block_name=testparams['block'])

    def test026e(self):
        """test026e: web.DBSReaderModel.listBlockSummaries: simple block example"""
        api.list('blocksummaries', block_name=testparams['block'])

    def test026f(self):
        """test026f: web.DBSReaderModel.listBlockSummaries: simple dataset example"""
        api.list('blocksummaries', dataset=testparams['dataset'])

    def test026g(self):
        """test026g: web.DBSReaderModel.listBlockSummaries: simple block_list example"""
        api.list('blocksummaries', block_name=[testparams['block'], testparams['block']])

    def test026e1(self):
        """test026e1: web.DBSReaderModel.listBlockSummaries: simple block with detail example"""
        api.list('blocksummaries', block_name=testparams['block'], detail=True)

    def test026f1(self):
        """test026f1: web.DBSReaderModel.listBlockSummaries: simple dataset with detail example"""
        api.list('blocksummaries', dataset=testparams['dataset'], detail=True)

    def test026g1(self):
        """test026g1: web.DBSReaderModel.listBlockSummaries: simple block_list with detail example"""
        api.list('blocksummaries', block_name=[testparams['block'], testparams['block']], detail=True)

    @checkException400
    def test027a(self):
        """test027a: web.DBSReaderModel.listRunSummaries: input validation test"""
        api.list('runsummaries')

    @checkException400
    def test027b(self):
        """test027a: web.DBSReaderModel.listRunSummaries: input validation test"""
        api.list('runsummaries', dataset=testparams['dataset'])

    @checkException400
    def test027c(self):
        """test027c: web.DBSReaderModel.listRunSummaries: input validation test"""
        api.list('runsummaries', dataset='/A/B*/C', run_num=testparams['run_num'])

    def test027d(self):
        """test027d: web.DBSReaderModel.listRunSummaries: simple run example"""
        api.list('runsummaries', run_num=testparams['run_num'])

    def test027e(self):
        """test027e: web.DBSReaderModel.listRunSummaries: simple run dataset example"""
        api.list('runsummaries', dataset=testparams['dataset'], run_num=testparams['run_num'])

    def test028a(self):
        """test028a: web.DBSReaderModel.getServerInfo: check that version is returned"""
        reg_ex = r'^(3+\.[0-9]+\.[0-9]+[\-\.a-z0-9]*$)'
        version = api.list('serverinfo')
        self.assertTrue('dbs_version' in version)
        self.assertFalse(re.compile(reg_ex).match(version['dbs_version']) is None)

if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSReaderModel_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
