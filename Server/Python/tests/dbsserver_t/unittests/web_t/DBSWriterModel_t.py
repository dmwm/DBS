"""
web unittests
"""

__revision__ = "$Id: DBSWriterModel_t.py,v 1.27 2010/08/24 19:48:44 yuyi Exp $"
__version__ = "$Revision: 1.27 $"

import os
import sys
import unittest
import time
import uuid
import traceback
from ctypes import *
from cherrypy import request, response, HTTPError
from dbsserver_t.utils.DBSRestApi import DBSRestApi
from WMCore.WebTools.FrontEndAuth import FrontEndAuth

config = os.environ["DBS_TEST_CONFIG"]
service = os.environ.get("DBS_TEST_SERVICE","DBSWriter")
api = DBSRestApi(config, service)
uid = uuid.uuid4().time_mid
print "****uid=%s******" %uid
acquisition_era_name="acq_era_%s" %uid
processing_version="%s" %(uid if (uid<9999) else uid%9999)
primary_ds_name = 'unittest_web_primary_ds_name_%s' % uid
procdataset = '%s-unittest_web_dataset-v%s' % (acquisition_era_name, processing_version)
childprocdataset = '%s-unittest_web_child_dataset-v%s' % (acquisition_era_name, processing_version)
tier = 'GEN-SIM-RAW'
dataset="/%s/%s/%s" % (primary_ds_name, procdataset, tier)
child_dataset="/%s/%s/%s" % (primary_ds_name, childprocdataset, tier)
app_name='cmsRun'
output_module_label='Merged-%s' %uid
global_tag='my_tag-%s'%uid
pset_hash='76e303993a1c2f842159dbfeeed9a0dd'
release_version='CMSSW_1_2_%s' % uid
site="cmssrm-%s.fnal.gov" %uid
block="%s#%s" % (dataset, uid)
child_block="%s#%s" % (child_dataset, uid)
run=uid
flist=[]
primary_ds_type='test'
prep_id = 'MC_12344'
child_prep_id = 'MC_3455'

outDict={
"primary_ds_name" : primary_ds_name,
"procdataset" : procdataset,
"tier" : tier,
"dataset" : dataset,
"child_dataset" : child_dataset,
"app_name" : app_name,
"output_module_label" : output_module_label,
"global_tag": global_tag,
"pset_hash" : pset_hash,
"release_version" : release_version,
"site" : site,
"block" : block,
"child_block" : child_block,
"files" : [],
"parent_files" : [],
"run":run,
"acquisition_era":acquisition_era_name,
"processing_version" : processing_version,
"primary_ds_type" : primary_ds_type,
"child_prep_id" : child_prep_id,
"prep_id" : prep_id
}

class checkException(object):
    def __init__(self,msg):
        self.msg = msg

    def __call__(self,func,*args,**kwargs):
        def wrapper(*args,**kwargs):
            out = None
            try:
                out = func(*args,**kwargs)
            except Exception, ex:
                test_class = args[0]
                if self.msg not in ex.args[0]:
                    test_class.fail("Exception was expected and was not raised")
            else:
                test_class.fail("Exception was expected and was not raised")

            return out
        return wrapper

class DBSWriterModel_t(unittest.TestCase):
    def test01a(self):
        """test01a: web.DBSWriterModel.insertPrimaryDataset: basic test\n"""
        data = {'primary_ds_name':primary_ds_name,
                'primary_ds_type':primary_ds_type}
        api.insert('primarydatasets', data)

    def test01b(self):
        """tes01b: web.DBSWriterModel.insertPrimaryDataset: duplicate should not raise an exception\n"""
        data = {'primary_ds_name':primary_ds_name,
                'primary_ds_type':primary_ds_type}
        api.insert('primarydatasets', data)

    @checkException("Primary dataset Name is required for insertPrimaryDataset")
    def test01c(self):
        """test01c: web.DBSWriterModel.insertPrimaryDataset: missing primary_ds_name, must throw exception\n"""

        data = {'primary_ds_type':primary_ds_type}
        junk = api.insert('primarydatasets', data)

    def test02a(self):
        """test 02a: web.DBSWriterModel.insertOutputModule: basic test"""
        data = {'release_version': release_version, 'pset_hash': pset_hash,
                'app_name': app_name, 'output_module_label': output_module_label, 'global_tag':global_tag}
        api.insert('outputconfigs', data)

    def test02b(self):
        """test02b: web.DBSWriterModel.insertOutputModule: re-insertion should not raise any errors"""
        data = {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                'output_module_label': output_module_label, 'global_tag':global_tag}
        api.insert('outputconfigs', data)

    @checkException("app_name")
    def test02c(self):
        """test02c: web.DBSWriterModel.insertOutputModule: missing parameter must cause an exception"""
        data = {'pset_hash': pset_hash,
                'output_module_label': output_module_label,
                'release_version': release_version}

        api.insert('outputconfigs', data)

    @checkException("pset_hash")
    def test02d(self):
        """test02d: web.DBSWriterModel.insertOutputModule: missing parameter must cause an exception"""
        data = {'app_name': app_name,
                'output_module_label': output_module_label,
                'release_version': release_version}

        api.insert('outputconfigs', data)

    @checkException("output_module_label")
    def test02e(self):
        """test02e: web.DBSWriterModel.insertOutputModule: missing parameter must cause an exception"""
        data = {'pset_hash': pset_hash,
                'app_name': app_name,
                'release_version': release_version}

        api.insert('outputconfigs', data)

    @checkException("release_version")
    def test02f(self):
        """test02f: web.DBSWriterModel.insertOutputModule: missing parameter must cause an exception"""
        data = {'pset_hash': pset_hash,
                'app_name': app_name,
                'output_module_label': output_module_label
                }

        api.insert('outputconfigs', data)

    def test03a(self):
        """test03a: web.DBSWriterModel.insertAcquisitionEra: Basic test """
        data={'acquisition_era_name': acquisition_era_name}
        api.insert('acquisitioneras', data)

    @checkException("acquisition_era_name")
    def test03b(self):
        """test03b: web.DBSWriterModel.insertAcquisitionEra: duplicate test """
        data={'acquisition_era_name': acquisition_era_name}
        api.insert('acquisitioneras', data)

    @checkException("acquisition_era_name")
    def test03c(self):
        """test03c: web.DBSWriterModel.insertAcquisitionEra: missing parameter should raise an exception """
        data={}

        api.insert('acquisitioneras', data)

    def test04a(self):
        """test04a: web.DBSWriterModel.insertProcessingEra: Basic test """
        data={'processing_version': processing_version, 'description':'this-is-a-test'}
        api.insert('processingeras', data)

    def test04b(self):
        """test04b: web.DBSWriterModel.insertProcessingEra: duplicate test """
        data={'processing_version': processing_version, 'description':'this-is-a-test'}
        api.insert('processingeras', data)

    @checkException("processing_version")
    def test04c(self):
        """test04c: web.DBSWriterModel.insertProcessingEra: duplicate test """
        data={'description':'this-is-a-test'}

        api.insert('processingeras', data)

    def test05a(self):
        """test05a: web.DBSWriterModel.insertDataset(Dataset is construct by DBSDatset.): basic test"""
        data = {
            'physics_group_name': 'Tracker', 'dataset': dataset,
            'dataset_access_type': 'PRODUCTION', 'processed_ds_name': procdataset, 'primary_ds_name': primary_ds_name,
            'output_configs': [
                {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                 'output_module_label': output_module_label, 'global_tag': global_tag},
                ],
            'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': tier,
            'prep_id':prep_id,
            'processing_version': processing_version,  'acquisition_era_name': acquisition_era_name
            }

        api.insert('datasets', data)
        childdata = {
            'physics_group_name': 'Tracker', 'dataset': child_dataset,
            'dataset_access_type': 'PRODUCTION', 'processed_ds_name': childprocdataset, 'primary_ds_name': primary_ds_name,
            'output_configs': [
                {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                 'output_module_label': output_module_label, 'global_tag': global_tag},
                ],
            'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': tier,
            'prep_id':child_prep_id,
            'processing_version': processing_version,  'acquisition_era_name': acquisition_era_name,
            }
        api.insert('datasets', childdata)

    def test05b(self):
        """test05b: web.DBSWriterModel.insertDataset: duplicate insert should be ignored"""
        data = {
            'physics_group_name': 'Tracker', 'dataset': dataset,
            'dataset_access_type': 'PRODUCTION', 'processed_ds_name': procdataset, 'primary_ds_name': primary_ds_name,
            'output_configs': [
                {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                 'output_module_label': output_module_label, 'global_tag': global_tag},
                ],
            'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': tier,
            'prep_id':prep_id,
            'processing_version': processing_version,  'acquisition_era_name': acquisition_era_name
            }

        api.insert('datasets', data)

    @checkException("primary_ds_name")
    def test05c(self):
        """test05c: web.DBSWriterModel.insertDataset: missing primary_ds_name must raise an error"""
        data = {
            'physics_group_name': 'Tracker', 'dataset': dataset,
            'dataset_access_type': 'PRODUCTION', 'processed_ds_name': procdataset,
            'output_configs': [
                {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                 'output_module_label': output_module_label, 'global_tag': global_tag},
                ],
            'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': tier,
            'prep_id':prep_id,
            'processing_version': processing_version,  'acquisition_era_name': acquisition_era_name
            }

        api.insert('datasets', data)

    @checkException("dataset_access_type")
    def test05d(self):
        """test05d: web.DBSWriterModel.insertDataset: missing parameter must raise an error"""
        data = {
            'physics_group_name': 'Tracker', 'dataset': dataset,
            'processed_ds_name': procdataset, 'primary_ds_name': primary_ds_name,
            'output_configs': [
                {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                 'output_module_label': output_module_label, 'global_tag': global_tag},
                ],
            'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': tier,
            'prep_id':prep_id,
            'processing_version': processing_version,  'acquisition_era_name': acquisition_era_name
            }

        api.insert('datasets', data)

    @checkException("dataset")
    def test05e(self):
        """test05e: web.DBSWriterModel.insertDataset: missing parameter must raise an error"""
        data = {
            'physics_group_name': 'Tracker',
            'dataset_access_type': 'PRODUCTION', 'processed_ds_name': procdataset, 'primary_ds_name': primary_ds_name,
            'output_configs': [
                {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                 'output_module_label': output_module_label, 'global_tag': global_tag},
                ],
            'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': tier,
            'prep_id':prep_id,
            'processing_version': processing_version,  'acquisition_era_name': acquisition_era_name
            }

        api.insert('datasets', data)

    @checkException("processed_ds_name")
    def test05f(self):
        """test05f: web.DBSWriterModel.insertDataset: missing parameter must raise an error"""
        data = {
            'physics_group_name': 'Tracker', 'dataset': dataset,
            'dataset_access_type': 'PRODUCTION', 'primary_ds_name': primary_ds_name,
            'output_configs': [
                {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                 'output_module_label': output_module_label, 'global_tag': global_tag},
                ],
            'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': tier,
            'prep_id':prep_id,
            'processing_version': processing_version,  'acquisition_era_name': acquisition_era_name
            }

        api.insert('datasets', data)

    @checkException("data_tier_name")
    def test05g(self):
        """test05g: web.DBSWriterModel.insertDataset: missing parameter must raise an error"""
        data = {
            'physics_group_name': 'Tracker', 'dataset': dataset,
            'dataset_access_type': 'PRODUCTION', 'processed_ds_name': procdataset, 'primary_ds_name': primary_ds_name,
            'output_configs': [
                {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                 'output_module_label': output_module_label, 'global_tag': global_tag},
                ],
            'xtcrosssection': 123, 'primary_ds_type': 'test',
            'prep_id':prep_id,
            'processing_version': processing_version,  'acquisition_era_name': acquisition_era_name
            }

        api.insert('datasets', data)

    @checkException("acquisition_era_name")
    def test05h(self):
        """test05h: web.DBSWriterModel.insertDataset: no output_configs, must raise an error!"""
        data = {
            'dataset': dataset,
            'physics_group_name': 'Tracker', 'primary_ds_name': primary_ds_name,
            'dataset_access_type': 'PRODUCTION', 'processed_ds_name': procdataset,
            'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': tier,
            'prep_id':prep_id
            }

        api.insert('datasets', data)

    def test06a(self):
        """test06a: web.DBSWriterModel.insertBlock: basic test"""
        data = {'block_name': block,
                'origin_site_name': site }

        api.insert('blocks', data)
        # insert the child block as well
        data = {'block_name': child_block, 'origin_site_name': site }
        api.insert('blocks', data)

    def test06b(self):
        """test06b: web.DBSWriterModel.insertBlock: duplicate insert should not raise exception"""
        data = {'block_name': block,
                'origin_site_name': site }

        api.insert('blocks', data)

    @checkException("block_name")
    def test06c(self):
        """test06c: web.DBSWriterModel.insertBlock: missing parameter should raise exception"""
        data = {'origin_site_name': site }

        api.insert('blocks', data)

    @checkException("origin_site_name")
    def test06d(self):
        """test06d: web.DBSWriterModel.insertBlock: missing parameter should raise exception"""
        data = {'block_name': block}

        api.insert('blocks', data)

    def test07a(self):
        """test07a: web.DBSWriterModel.insertFiles: basic test"""
        data={}
        flist=[]
        for i in range(10):
            f={
                'adler32': '', 'file_type': 'EDM',
                'file_output_config_list':
                [
                    {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                     'output_module_label': output_module_label, 'global_tag': global_tag},
                    ],
                'dataset': dataset,
                'file_size': u'2012211901', 'auto_cross_section': 0.0,
                'check_sum': u'1504266448',
                'file_lumi_list': [
                    {'lumi_section_num': u'27414', 'run_num': uid},
                    {'lumi_section_num': u'26422', 'run_num': uid},
                    {'lumi_section_num': u'29838', 'run_num': uid}
                    ],
                'file_parent_list': [ ],
                'event_count': u'1619',
                'logical_file_name': "/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/%s/%i.root" %(uid,i),
                'block_name': block,
                #'is_file_valid': 1
                }
            flist.append(f)
        data={"files":flist}
        api.insert('files', data)

    def test07b(self):
        """test07b: web.DBSWriterModel.insertFiles: duplicate insert file shuld not raise any errors"""
        data={}
        flist=[]
        for i in range(10):
            f={
                'adler32': '', 'file_type': 'EDM',
                'file_output_config_list':
                [
                    {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                     'output_module_label': output_module_label, 'global_tag': global_tag},
                    ],
                'dataset': dataset,
                'file_size': u'2012211901', 'auto_cross_section': 0.0,
                'check_sum': u'1504266448',
                'file_lumi_list': [
                    {'lumi_section_num': u'27414', 'run_num': u'1'},
                    {'lumi_section_num': u'26422', 'run_num': u'1'},
                    {'lumi_section_num': u'29838', 'run_num': u'1'}
                    ],
                'file_parent_list': [ ],
                'event_count': u'1619',
                'logical_file_name': "/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/%s/%i.root" %(uid,i),
                'block_name': block,
                #'is_file_valid': 1
                }
            flist.append(f)
            outDict['files'].append(f['logical_file_name'])
        data={"files":flist}
        api.insert('files', data)

    def test07c(self):
        """test07c: web.DBSWriterModel.insertFiles: with parents"""
        data={}
        flist=[]

        for i in range(10):
            f={
                'adler32': u'NOSET', 'file_type': 'EDM',
                'file_output_config_list':
                [
                    {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                     'output_module_label': output_module_label, 'global_tag': global_tag},
                    ],
                'dataset': child_dataset,
                'file_size': u'2012211901', 'auto_cross_section': 0.0,
                'check_sum': u'1504266448',
                'file_lumi_list': [
                    {'lumi_section_num': u'27414', 'run_num': u'1'},
                    {'lumi_section_num': u'26422', 'run_num': u'1'},
                    {'lumi_section_num': u'29838', 'run_num': u'1'}
                    ],
                'file_parent_list': [{"file_parent_lfn": "/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/%s/%i.root" %(uid, i)}],
                'event_count': u'1619',
                'logical_file_name': "/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL-child/%s/%i.root" %(uid, i),
                'block_name': child_block
                #'is_file_valid': 1
                }
            flist.append(f)
            outDict['files'].append(f['logical_file_name'])
            outDict['parent_files'].append(f['file_parent_list'][0]['file_parent_lfn'])
        data={"files":flist}
        api.insert('files', data)

    @checkException("logical_file_name")
    def test07d(self):
        """test07d: web.DBSWriterModel.insertFiles: missing parameter should raise an exception"""
        data={}
        flist=[]
        for i in range(10):
            f={
                'adler32': '', 'file_type': 'EDM',
                'file_output_config_list':
                [
                    {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                     'output_module_label': output_module_label, 'global_tag': global_tag},
                    ],
                'dataset': dataset,
                'file_size': u'2012211901', 'auto_cross_section': 0.0,
                'check_sum': u'1504266448',
                'file_lumi_list': [
                    {'lumi_section_num': u'27414', 'run_num': u'1'},
                    {'lumi_section_num': u'26422', 'run_num': u'1'},
                    {'lumi_section_num': u'29838', 'run_num': u'1'}
                    ],
                'file_parent_list': [ ],
                'event_count': u'1619',
                'block_name': block,
                #'is_file_valid': 1
                }
            flist.append(f)
        data={"files":flist}

        api.insert('files', data)

    @checkException("block_name")
    def test07e(self):
        """test07e: web.DBSWriterModel.insertFiles: missing parameter should raise an exception"""
        data={}
        flist=[]
        for i in range(10):
            f={
                'adler32': '', 'file_type': 'EDM',
                'file_output_config_list':
                [
                    {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                     'output_module_label': output_module_label, 'global_tag': global_tag},
                    ],
                'dataset': dataset,
                'file_size': u'2012211901', 'auto_cross_section': 0.0,
                'check_sum': u'1504266448',
                'file_lumi_list': [
                    {'lumi_section_num': u'27414', 'run_num': u'1'},
                    {'lumi_section_num': u'26422', 'run_num': u'1'},
                    {'lumi_section_num': u'29838', 'run_num': u'1'}
                    ],
                'file_parent_list': [ ],
                'event_count': u'1619',
                'logical_file_name': "/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/%s/%i.root" %(uid,i),
                #'is_file_valid': 1
                }
            flist.append(f)
        data={"files":flist}

        api.insert('files', data)

    @checkException("dataset")
    def test07f(self):
        """test07f: web.DBSWriterModel.insertFiles: missing parameter should raise an exception"""
        data={}
        flist=[]
        for i in range(10):
            f={
                'adler32': '', 'file_type': 'EDM',
                'file_output_config_list':
                [
                    {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                     'output_module_label': output_module_label, 'global_tag': global_tag},
                    ],
                'file_size': u'2012211901', 'auto_cross_section': 0.0,
                'check_sum': u'1504266448',
                'file_lumi_list': [
                    {'lumi_section_num': u'27414', 'run_num': u'1'},
                    {'lumi_section_num': u'26422', 'run_num': u'1'},
                    {'lumi_section_num': u'29838', 'run_num': u'1'}
                    ],
                'file_parent_list': [ ],
                'event_count': u'1619',
                'logical_file_name': "/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/%s/%i.root" %(uid,i),
                'block_name': block,
                #'is_file_valid': 1
                }
            flist.append(f)
        data={"files":flist}

        api.insert('files', data)

    def test07g(self):
        """test07g: web.DBSWriterModel.insertFiles: minimal set of parameter  should not raise an exception"""
        data={}
        flist=[]
        for i in range(10):
            f={
                'dataset': dataset,
                'logical_file_name': "/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/%s/%i.root" %(uid,i),
                'block_name': block,
                #'is_file_valid': 1
                }
            flist.append(f)
        data={"files":flist}

        api.insert('files', data)

    def test08a(self):
        """test08a: testweb.DBSWriterModel.insertDataTier: Basic test"""
        data = {'data_tier_name':tier}
        api.insert('datatiers',data)

    @checkException("data_tier_name")
    def test08b(self):
        """test08b: web.DBSWriterModel.insertDataTier: missing data should raise exception"""
        data = {}

        api.insert('datatiers',data)

    def test09a(self):
        """test09a: web.DBSWriterModel.updateDatasetType: Basic test """
        api.update('datasets', dataset=dataset, dataset_access_type="DEPRECATED")

    @checkException("dataset_access_type")
    def test09b(self):
        """test22a web.DBSWriterModel.updateDatasetType: Basic test """

        api.update('datasets')

    @checkException("dataset")
    def test09c(self):
        """test09c: web.DBSWriterModel.updateDatasetType: Basic test """

        api.update('datasets',dataset_access_type="DEPRECATED")

    def test10a(self):
        """test10a: web.DBSWriterModel.updateFileStatus: Basic test logical_file_name"""
        lfn = "/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL-child/%s/%i.root" %(uid, 1)
        api.update('files', logical_file_names=lfn, is_file_valid=0)

    def test10b(self):
        """test10b: web.DBSWriterModel.updateFileStatus: Basic test logical_file_name list"""
        lfn = ["/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL-child/%s/%i.root" % (uid, 1),
               "/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL-child/%s/%i.root" % (uid, 2)]
        api.update('files', logical_file_names=lfn, is_file_valid=0)

    def test10c(self):
        """test10c: web.DBSWriterModel.updateFileStatus: Basic test logical_file_name and lost"""
        lfn = "/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL-child/%s/%i.root" %(uid, 1)
        api.update('files', logical_file_names=lfn, is_file_valid=0, lost=1)

    def test10d(self):
        """test10d: web.DBSWriterModel.updateFileStatus: Basic test logical_file_name list and lost"""
        lfn = ["/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL-child/%s/%i.root" % (uid, 1),
               "/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL-child/%s/%i.root" % (uid, 2)]
        api.update('files', logical_file_names=lfn, is_file_valid=0, lost=1)

    def test11a(self):
        """test11a: web.DBSWriterModel.updateBlock: Basic test"""
        api.update('blocks', block_name=block, open_for_writing=0)

    @checkException("block_name")
    def test11b(self):
        """test11b: web.DBSWriterModel.updateBlock: missing data should raise exception"""
        api.update('blocks')

    @checkException("already exists")
    def test12a(self):
        """test12a: web.DBSWriterModel.insertBulkBlock: existing block will raise an exception"""
        dataset_dict = {'dataset': dataset,
                        'physics_group_name': 'Tracker', 'primary_ds_name': primary_ds_name,
                        'dataset_access_type': 'PRODUCTION', 'processed_ds_name': procdataset,
                        'xtcrosssection': 123, 'primary_ds_type': primary_ds_type, 'data_tier_name': tier,
                        'prep_id':prep_id}

        block_dict = data = {'block_name': block,
                             'origin_site_name': site}

        processing_dict = {'processing_version': processing_version,
                           'description':'this-is-a-test'}

        acquisition_dict = {'acquisition_era_name': acquisition_era_name}

        primary_dict = {'primary_ds_name':primary_ds_name,
                'primary_ds_type':primary_ds_type}

        output_module_dict = {'release_version': release_version, 'pset_hash': pset_hash,
                              'app_name': app_name, 'output_module_label': output_module_label,
                              'global_tag':global_tag}

        file_output_dict = {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                            'output_module_label': output_module_label, 'global_tag': global_tag}
        fileList=[]

        for i in range(10):
            f={
                'adler32': '', 'file_type': 'EDM',
                'dataset': dataset,
                'file_size': u'2012211901', 'auto_cross_section': 0.0,
                'check_sum': u'1504266448',
                'file_lumi_list': [
                    {'lumi_section_num': u'27414', 'run_num': u'1'},
                    {'lumi_section_num': u'26422', 'run_num': u'1'},
                    {'lumi_section_num': u'29838', 'run_num': u'1'}
                    ],
                'file_parent_list': [ ],
                'event_count': u'1619',
                'logical_file_name': "/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/%s/%i.root" %(uid,i),
                }
            fileList.append(f)

        data = {'file_conf_list' : [file_output_dict],
                'dataset_conf_list' : [output_module_dict],
                'block_parent_list' : [],
                'processing_era' : processing_dict,
                'files' : fileList,
                'dataset' : dataset_dict,
                'primds' : primary_dict,
                'acquisition_era' : acquisition_dict,
                'ds_parent_list' : [],
                'block' : block_dict,
                'file_parent_list' : []}

        api.insert('bulkblocks', data)

    def test12b(self):
        """test12b: web.DBSWriterModel.insertBulkBlock: basic test"""
        uniq_id = int(time.time())

        bulk_primary_ds_name = 'unittest_web_primary_ds_name_%s' % (uniq_id)
        bulk_procdataset = '%s-unittest_web_dataset-v%s' % (acquisition_era_name, processing_version)

        bulk_dataset = '/%s/%s/%s' % (bulk_primary_ds_name,
                                      bulk_procdataset,
                                      tier)
        bulk_block="%s#%s" % (dataset, uniq_id)

        dataset_dict = {u'dataset': bulk_dataset,
                        u'physics_group_name': 'Tracker',
                        u'dataset_access_type': 'PRODUCTION', u'processed_ds_name': bulk_procdataset,
                        u'xtcrosssection': 123, u'data_tier_name': tier,
                        u'prep_id':prep_id}

        block_dict = data = {u'block_name': bulk_block,
                             u'origin_site_name': site}

        processing_dict = {u'processing_version': processing_version,
                           u'description':'this-is-a-test'}

        acquisition_dict = {u'acquisition_era_name': acquisition_era_name, u'start_date':1234567890}

        primary_dict = {u'primary_ds_name':bulk_primary_ds_name,
                        u'primary_ds_type':primary_ds_type}

        output_module_dict = {u'release_version': release_version, u'pset_hash': pset_hash,
                              u'app_name': app_name, u'output_module_label': output_module_label,
                              u'global_tag':global_tag}

        fileList = []
        fileConfigList = []

        for i in range(10):
            f={
                u'adler32': 'NOTSET', u'file_type': 'EDM',
                u'file_size': '2012211901', u'auto_cross_section': 0.0,
                u'check_sum': '1504266448',
                u'file_lumi_list': [
                    {u'lumi_section_num': '27414', u'run_num': '1'},
                    {u'lumi_section_num': '26422', u'run_num': '1'},
                    {u'lumi_section_num': '29838', u'run_num': '1'}
                    ],
                u'event_count': u'1619',
                u'logical_file_name': "/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/%s/%i.root" %(uniq_id,i),
                }
            fileList.append(f)

            file_output_dict = {u'release_version': release_version, u'pset_hash': pset_hash, u'app_name': app_name,
                                u'output_module_label': output_module_label, u'global_tag': global_tag, u'lfn':f["logical_file_name"]}
            fileConfigList.append(file_output_dict)

        data = {'file_conf_list' : fileConfigList,
                'dataset_conf_list' : [output_module_dict],
                'processing_era' : processing_dict,
                'files' : fileList,
                'dataset' : dataset_dict,
                'primds' : primary_dict,
                'acquisition_era' : acquisition_dict,
                'block' : block_dict,
                }

        api.insert('bulkblocks', data)

    def test999(self):
        """setup all necessary parameters"""
        filename=os.path.join(os.path.dirname(os.path.abspath(__file__)),'info.dict')
        infoout=open(filename, "w")
        infoout.write("info="+str(outDict))
        infoout.close()


if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSWriterModel_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
