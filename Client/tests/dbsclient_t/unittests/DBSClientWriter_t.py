"""
client writer unittests
"""

import os
import sys
import time
import uuid
import unittest
from dbs.apis.dbsClient import *
from ctypes import *

uid = uuid.uuid4().time_mid
print("****uid=%s******" %uid)

print(os.environ['DBS_WRITER_URL'])
primary_ds_name = 'unittest_web_primary_ds_name_%s' % uid
processing_version="%s" %(uid if (uid<9999) else uid%9999)
acquisition_era_name="acq_era_%s" %uid
procdataset = '%s-v%s' % (acquisition_era_name, processing_version)
parent_procdataset = '%s-pstr-v%s' % (acquisition_era_name, processing_version)
tier = 'GEN-SIM-RAW'
dataset="/%s/%s/%s" % (primary_ds_name, procdataset, tier)
dataset2="/%s_2/%s/%s" % (primary_ds_name, procdataset, tier)
app_name='cmsRun'
output_module_label='Merged'
global_tag='my-cms-gtag_%s' % uid
pset_hash='76e303993a1c2f842159dbfeeed9a0dd'
pset_name='UnitestPsetName'
release_version='CMSSW_1_2_3'
site="cmssrm.fnal.gov"
block="%s#%s" % (dataset, uid)
parent_dataset="/%s/%s/%s" % (primary_ds_name, parent_procdataset, tier)
parent_block="%s#%s" % (parent_dataset, uid)
print("parent_block = ", parent_block)
print("block = ", block)
flist=[]

outDict={
"primary_ds_name": primary_ds_name,
"procdataset": procdataset,
"tier": tier,
"dataset": dataset,
"parent_dataset": parent_dataset,
"app_name": app_name,
"output_module_label": output_module_label,
"global_tag": global_tag,
"pset_hash": pset_hash,
"release_version": release_version,
"site": site,
"block": block,
"parent_block": parent_block,
"files": [],
"parent_files": [],
"runs": [97, 98, 99],
"acquisition_era": acquisition_era_name,
"processing_version": processing_version,
}

class DBSClientWriter_t(unittest.TestCase):

    def __init__(self, methodName='runTest'):
        super(DBSClientWriter_t, self).__init__(methodName)
        url=os.environ['DBS_WRITER_URL']
        proxy=os.environ.get('SOCKS5_PROXY')
        self.api = DbsApi(url=url, proxy=proxy)

    def setUp(self):
        """setup all necessary parameters"""

    def test01(self):
        """test01: web.DBSClientWriter.insertPrimaryDataset: basic test"""
        data = {'primary_ds_name':primary_ds_name,
                'primary_ds_type':'test'}
        self.api.insertPrimaryDataset(primaryDSObj=data)

    def test02(self):
        """test02: web.DBSClientWriter.insertPrimaryDataset: duplicate should not riase an exception"""
        data = {'primary_ds_name':primary_ds_name,
                'primary_ds_type':'test'}
        self.api.insertPrimaryDataset(primaryDSObj=data)

    def test04(self):
        """test04: web.DBSClientWriter.insertOutputModule: basic test"""
        data = {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                'output_module_label': output_module_label, 'global_tag':global_tag}
        self.api.insertOutputConfig(outputConfigObj=data)

    def test05(self):
        """test05: web.DBSClientWriter.insertOutputModule: re-insertion should not raise any errors"""
        data = {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                'output_module_label': output_module_label, 'global_tag':global_tag, 'pset_name':pset_name}
        self.api.insertOutputConfig(outputConfigObj=data)

    def test06(self):
        """test06: web.DBSWriterModel.insertAcquisitionEra: Basic test """
        data={'acquisition_era_name': acquisition_era_name}
        self.api.insertAcquisitionEra(data)

    def test07(self):
        """test07: web.DBSWriterModel.insertProcessingEra: Basic test """
        data={'processing_version': processing_version, 'description':'this_is_a_test'}
        self.api.insertProcessingEra(data)

    def test08(self):
        """test08: web.DBSClientWriter.insertDataset: basic test"""
        data = {
            'physics_group_name': 'Tracker', 'dataset': dataset,
            'dataset_access_type': 'PRODUCTION', 'processed_ds_name': procdataset, 'primary_ds_name': primary_ds_name,
            'output_configs': [
                {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                 'output_module_label': output_module_label, 'global_tag':global_tag}
                ],
            'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': tier,
            'creation_date': 1234, 'create_by': 'anzar', "last_modification_date": 1234, "last_modified_by": "testuer",
            'processing_version': processing_version,  'acquisition_era_name': acquisition_era_name,
            }

        self.api.insertDataset(datasetObj=data)
        # insert away the parent dataset as well

        parentdata = {
            'physics_group_name': 'Tracker', 'dataset': parent_dataset,
            'dataset_access_type': 'PRODUCTION', 'processed_ds_name': parent_procdataset, 'primary_ds_name': primary_ds_name,
            'output_configs': [
                {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                 'output_module_label': output_module_label, 'global_tag':global_tag}
                ],
            'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': tier,
            'creation_date': 1234, 'create_by': 'anzar', "last_modification_date": 1234, "last_modified_by": "testuser",
            'processing_version': processing_version,  'acquisition_era_name': acquisition_era_name,
            }
        self.api.insertDataset(datasetObj=parentdata)

    def test09(self):
        """test09: web.DBSClientWriter.insertDataset: duplicate insert should be ignored"""
        data = {
            'physics_group_name': 'Tracker', 'dataset': dataset,
            'dataset_access_type': 'PRODUCTION', 'processed_ds_name': procdataset, 'primary_ds_name': primary_ds_name,
            'output_configs': [
                {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                 'output_module_label': output_module_label, 'global_tag':global_tag},
                ],
            'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': tier,
            'creation_date': 1234, 'create_by': 'anzar', "last_modification_date": 1234, "last_modified_by": "anzar",
            'processing_version': processing_version,  'acquisition_era_name': acquisition_era_name,
            }

        self.api.insertDataset(datasetObj=data)

    def test11(self):
        """test11: web.DBSClientWriter.insertDataset: no output_configs, should be fine insert!"""
        data = {
            'dataset': dataset2,
            'physics_group_name': 'Tracker', 'primary_ds_name': primary_ds_name,
            'dataset_access_type': 'PRODUCTION', 'processed_ds_name': procdataset,
            'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': tier,
            'creation_date': 1234, 'create_by': 'testuser', "last_modification_date": 1234, "last_modified_by"
            : "testuser",
            'processing_version': processing_version,  'acquisition_era_name': acquisition_era_name,
            }
        self.api.insertDataset(datasetObj=data)

    def test14(self):
        """test14 web.DBSClientWriter.insertBlock: basic test"""
        data = {'block_name': block,
                'origin_site_name': site }

        self.api.insertBlock(blockObj=data)
        # insert the parent block as well
        data = {'block_name': parent_block, 'origin_site_name': site }
        self.api.insertBlock(blockObj=data)

    def test15(self):
        """test15 web.DBSClientWriter.insertBlock: duplicate insert should not raise exception"""
        data = {'block_name': block,
                'origin_site_name': site }

        self.api.insertBlock(blockObj=data)

    def test16(self):
        """test16 web.DBSClientWriter.insertFiles: insert parent file for later use : basic test"""
        flist=[]
        for i in range(10):
            f={
                'adler32': 'NOTSET', 'file_type': 'EDM',
                'file_output_config_list':
                [
                    {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                     'output_module_label': output_module_label,'global_tag':global_tag },
                    ],
                'dataset': parent_dataset,
                'file_size': '2012211901', 'auto_cross_section': 0.0,
                'check_sum': '1504266448',
                'file_lumi_list': [
                    {'lumi_section_num': '27414', 'run_num': '97', 'event_count': '66'},
                    {'lumi_section_num': '26422', 'run_num': '97', 'event_count': '67'},
                    {'lumi_section_num': '29838', 'run_num': '97', 'event_count': '68'},
                    {'lumi_section_num': '248', 'run_num': '97',   'event_count': '69'},
                    {'lumi_section_num': '250', 'run_num': '97', 'event_count': '70'},
                    {'lumi_section_num': '300', 'run_num': '97', 'event_count': '71'},
                    {'lumi_section_num': '534', 'run_num': '97', 'event_count': '72'},
                    {'lumi_section_num': '546', 'run_num': '97', 'event_count': '73'},
                    {'lumi_section_num': '638', 'run_num': '97', 'event_count': '74'},
                    {'lumi_section_num': '650', 'run_num': '97', 'event_count': '75'},
                    {'lumi_section_num': '794', 'run_num': '97', 'event_count': '76'},
                    {'lumi_section_num': '1313', 'run_num': '97', 'event_count': '77'},
                    {'lumi_section_num': '1327', 'run_num': '97', 'event_count': '78'},
                    {'lumi_section_num': '1339', 'run_num': '97', 'event_count': '79'},
                    {'lumi_section_num': '1353', 'run_num': '97', 'event_count': '80'},
                    {'lumi_section_num': '1428', 'run_num': '97', 'event_count': '81'},
                    {'lumi_section_num': '1496', 'run_num': '97', 'event_count': '82'},
                    {'lumi_section_num': '1537', 'run_num': '97', 'event_count': '83'},
                    {'lumi_section_num': '1652', 'run_num': '97', 'event_count': '84'},
                    {'lumi_section_num': '1664', 'run_num': '97', 'event_count': '85'},
                    {'lumi_section_num': '1743', 'run_num': '97', 'event_count': '86'},
                    {'lumi_section_num': '1755', 'run_num': '97', 'event_count': '87'},
                    {'lumi_section_num': '1860', 'run_num': '97', 'event_count': '88'},
                    {'lumi_section_num': '1872', 'run_num': '97', 'event_count': '89'}
                    ],
                'file_parent_list': [ ],
                'event_count': '1619',
                'logical_file_name': "/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/p%s/%i.root" %(uid, i),
                'block_name': parent_block
                #'is_file_valid': 1
                }
            flist.append(f)
        self.api.insertFiles(filesList={"files":flist})

    def test17(self):
        """test17 web.DBSClientWriter.insertFiles: basic test"""

        flist=[]
        for i in range(10):
            f={
                'adler32': 'NOTSET', 'file_type': 'EDM',
                'file_output_config_list':
                [
                    {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                     'output_module_label': output_module_label, 'global_tag':global_tag},
                    ],
                'dataset': dataset,
                'file_size': '2012211901', 'auto_cross_section': 0.0,
                'check_sum': '1504266448',
                'file_lumi_list': [
                    {'lumi_section_num': '27414', 'run_num': '97'},
                    {'lumi_section_num': '26422', 'run_num': '98'},
                    {'lumi_section_num': '29838', 'run_num': '99'}
                    ],
                'file_parent_list': [ {"file_parent_lfn" : "/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/p%s/%i.root" %(uid, i)} ],
                'event_count': '1619',
                'logical_file_name': "/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/%s/%i.root" %(uid, i),
                'block_name': block
                #'is_file_valid': 1
                }
            flist.append(f)
            outDict['parent_files'].append(f['file_parent_list'][0]['file_parent_lfn'])
        self.api.insertFiles(filesList={"files":flist})

    def test18(self):
        """test18 web.DBSClientWriter.insertFiles: duplicate insert file shuld not raise any errors"""
        flist=[]
        for i in range(10):
            f={
                'adler32': 'NOTSET', 'file_type': 'EDM',
                'file_output_config_list':
                [
                    {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                     'output_module_label': output_module_label, 'global_tag':global_tag},
                    ],
                'dataset': dataset,
                'file_size': '2012211901', 'auto_cross_section': 0.0,
                'check_sum': '1504266448',
                'file_lumi_list': [
                    {'lumi_section_num': '27414', 'run_num': '97'},
                    {'lumi_section_num': '26422', 'run_num': '98'},
                    {'lumi_section_num': '29838', 'run_num': '99'}
                    ],
                'file_parent_list': [ {"file_parent_lfn" : "/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/p%s/%i.root" %(uid, i)} ],
                'event_count': '1619',
                'logical_file_name': "/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/%s/%i.root" %(uid, i),
                'block_name': block
                #'is_file_valid': 1
                }
            flist.append(f)
            outDict['files'].append(f['logical_file_name'])
        self.api.insertFiles(filesList={"files":flist})

    def test19(self):
        """test19 web.DBSClientWriter.updateFileStatus: should be able to update file status"""
        logical_file_name = "/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/%s/%i.root" %(uid, 1)
        self.api.updateFileStatus(logical_file_name=logical_file_name, is_file_valid=0)

    def test20(self):
        """test20 web.DBSClientWriter.updateDatasetType: should be able to update dataset type"""
        self.api.updateDatasetType(dataset=dataset, dataset_access_type="VALID")

    def test21(self):
        """test21 web.DBSClientWriter.updateBlockStatus: should be able to update block status"""
        self.api.updateBlockStatus(block_name=block, open_for_writing=0)

    def test22(self):
        """test22 web.DBSClientWriter.updateBlockSiteName: should be able to update origin_site_name"""
        self.api.updateBlockSiteName(block_name=block, origin_site_name=site)

    def test208(self):
        """test208 generating the output file for reader test"""
        infoout=open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "info.dict"), "w")
        infoout.write("info="+str(outDict))
        infoout.close()

if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSClientWriter_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
