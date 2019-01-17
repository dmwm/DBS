"""
client writer unittests
"""
from __future__ import print_function
import os
import sys
import time
import uuid
import unittest
import copy
from dbs.apis.dbsClient import *
from dbs.exceptions.dbsClientException import dbsClientException
from RestClient.ErrorHandling.RestClientExceptions import HTTPError
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
primary_ds_name2 = "%s_2" % primary_ds_name
dataset2="/%s/%s/%s" % (primary_ds_name2, procdataset, tier)
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
step_primary_ds_name = "%s_stepchain" % primary_ds_name
stepchain_dataset = "/%s/%s/%s" % (step_primary_ds_name, procdataset, tier)
stepchain_block="%s#%s" % (stepchain_dataset, uid)
parent_stepchain_dataset="/%s/%s/%s" % (step_primary_ds_name, parent_procdataset, tier)
parent_stepchain_block="%s#%s" % (parent_stepchain_dataset, uid)
print("parent_stepchain_block = ", parent_block)
print("stepchain_block = ", block)
flist=[]
stepchain_files = []
parent_stepchain_files = []

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
"step_primary_ds_name": step_primary_ds_name,
"stepchain_dataset": stepchain_dataset,
"stepchain_block": stepchain_block,
"parent_stepchain_dataset": parent_stepchain_dataset,
"parent_stepchain_block": parent_stepchain_block,
"stepchain_files": stepchain_files,
"parent_stepchain_files": parent_stepchain_files
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
        data = {'primary_ds_name': primary_ds_name2,
                'primary_ds_type': 'test'}
        self.api.insertPrimaryDataset(primaryDSObj=data)
        data = {
            'dataset': dataset2,
            'physics_group_name': 'Tracker', 'primary_ds_name': primary_ds_name2,
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
                'adler32': u'NOTSET', 'file_type': 'EDM',
                'file_output_config_list':
                [
                    {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                     'output_module_label': output_module_label,'global_tag':global_tag },
                    ],
                'dataset': parent_dataset,
                'file_size': u'2012211901', 'auto_cross_section': 0.0,
                'check_sum': u'1504266448',
                'file_lumi_list': [
                    {'lumi_section_num': u'27414', 'run_num': u'97', 'event_count': u'66'},
                    {'lumi_section_num': u'26422', 'run_num': u'97', 'event_count': u'67'},
                    {'lumi_section_num': u'29838', 'run_num': u'97', 'event_count': u'68'},
                    {'lumi_section_num': u'248', 'run_num': u'97',   'event_count': u'69'},
                    {'lumi_section_num': u'250', 'run_num': u'97', 'event_count': u'70'},
                    {'lumi_section_num': u'300', 'run_num': u'97', 'event_count': u'71'},
                    {'lumi_section_num': u'534', 'run_num': u'97', 'event_count': u'72'},
                    {'lumi_section_num': u'546', 'run_num': u'97', 'event_count': u'73'},
                    {'lumi_section_num': u'638', 'run_num': u'97', 'event_count': u'74'},
                    {'lumi_section_num': u'650', 'run_num': u'97', 'event_count': u'75'},
                    {'lumi_section_num': u'794', 'run_num': u'97', 'event_count': u'76'},
                    {'lumi_section_num': u'1313', 'run_num': u'97', 'event_count': u'77'},
                    {'lumi_section_num': u'1327', 'run_num': u'97', 'event_count': u'78'},
                    {'lumi_section_num': u'1339', 'run_num': u'97', 'event_count': u'79'},
                    {'lumi_section_num': u'1353', 'run_num': u'97', 'event_count': u'80'},
                    {'lumi_section_num': u'1428', 'run_num': u'97', 'event_count': u'81'},
                    {'lumi_section_num': u'1496', 'run_num': u'97', 'event_count': u'82'},
                    {'lumi_section_num': u'1537', 'run_num': u'97', 'event_count': u'83'},
                    {'lumi_section_num': u'1652', 'run_num': u'97', 'event_count': u'84'},
                    {'lumi_section_num': u'1664', 'run_num': u'97', 'event_count': u'85'},
                    {'lumi_section_num': u'1743', 'run_num': u'97', 'event_count': u'86'},
                    {'lumi_section_num': u'1755', 'run_num': u'97', 'event_count': u'87'},
                    {'lumi_section_num': u'1860', 'run_num': u'97', 'event_count': u'88'},
                    {'lumi_section_num': u'1872', 'run_num': u'97', 'event_count': u'89'}
                    ],
                'file_parent_list': [ ],
                'event_count': u'1619',
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
                'adler32': u'NOTSET', 'file_type': 'EDM',
                'file_output_config_list':
                [
                    {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                     'output_module_label': output_module_label, 'global_tag':global_tag},
                    ],
                'dataset': dataset,
                'file_size': u'2012211901', 'auto_cross_section': 0.0,
                'check_sum': u'1504266448',
                'file_lumi_list': [
                    {'lumi_section_num': u'27414', 'run_num': u'97'},
                    {'lumi_section_num': u'26422', 'run_num': u'98'},
                    {'lumi_section_num': u'29838', 'run_num': u'99'}
                    ],
                'file_parent_list': [ {"file_parent_lfn" : "/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/p%s/%i.root" %(uid, i)} ],
                'event_count': u'1619',
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
                'adler32': u'NOTSET', 'file_type': 'EDM',
                'file_output_config_list':
                [
                    {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                     'output_module_label': output_module_label, 'global_tag':global_tag},
                    ],
                'dataset': dataset,
                'file_size': u'2012211901', 'auto_cross_section': 0.0,
                'check_sum': u'1504266448',
                'file_lumi_list': [
                    {'lumi_section_num': u'27414', 'run_num': u'97'},
                    {'lumi_section_num': u'26422', 'run_num': u'98'},
                    {'lumi_section_num': u'29838', 'run_num': u'99'}
                    ],
                'file_parent_list': [ {"file_parent_lfn" : "/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/p%s/%i.root" %(uid, i)} ],
                'event_count': u'1619',
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

    def test23(self):
        """test23 web.DBSClientWriter.insertFiles: insert parent and child files for later use : basic test"""

        data = {'dataset_conf_list': [],  # List of dataset configurations
                'file_conf_list': [],  # List of files, the configuration for each
                'files': [],  # List of file objects
                'block': {},  # Dict of block info
                'processing_era': {},  # Dict of processing era info
                'acquisition_era': {},  # Dict of acquisition era information
                'primds': {},  # Dict of primary dataset info
                'dataset': {},  # Dict of processed dataset info
                #'file_parent_list': [],  # List of file parents
                'dataset_parent_list': [], # List of dataset parents
                }
                #'close_settings': {}}  # Dict of info about block close settings

        algo = {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                 'output_module_label': output_module_label, 'global_tag': global_tag}
        data['dataset_conf_list'] = [algo]

        data['primds']['primary_ds_name'] = step_primary_ds_name
        data['primds']['primary_ds_type'] = "test"
        data['primds']['create_by'] = "WMAgent"
        data['primds']['creation_date'] = int(time.time())

        # Do the processed
        data['dataset']['physics_group_name'] = 'Tracker'
        data['dataset']['processed_ds_name'] = procdataset
        data['dataset']['data_tier_name'] = tier
        data['dataset']['dataset_access_type'] = 'PRODUCTION'
        data['dataset']['dataset'] = stepchain_dataset
        data['dataset']['prep_id'] = "TestPrepID"
        # Add misc meta data.
        data['dataset']['create_by'] = "WMAgent"
        data['dataset']['last_modified_by'] = "WMAgent"
        data['dataset']['creation_date'] = int(time.time())
        data['dataset']['last_modification_date'] = int(time.time())
        data["processing_era"]["processing_version"] = processing_version
        data["processing_era"]["create_by"] = "WMAgent"

        data['acquisition_era']['acquisition_era_name'] = acquisition_era_name
        data['acquisition_era']['start_date'] = 123456789
        data['dataset_parent_list'] = [parent_stepchain_dataset]

        data['block']['block_name'] = stepchain_block
        data['block']['origin_site_name'] = site
        fCount = 5
        data['block']['file_count'] = fCount
        data['block']['block_size'] = 20122119010


        parent_data = copy.deepcopy(data)
        parent_data['dataset']['dataset'] = parent_stepchain_dataset
        parent_data['block']['block_name'] = parent_stepchain_block
        parent_data['dataset_parent_list'] = []
        parent_data['primds']['primary_ds_name'] = step_primary_ds_name
        parent_data['dataset']['processed_ds_name'] = parent_procdataset
        pflist=[]
        cflist=[]
        for i in range(fCount):
            f={
                'adler32': u'NOTSET', 'file_type': 'EDM',
                #'file_output_config_list':
                #[
                #    {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                #     'output_module_label': output_module_label,'global_tag':global_tag },
                #    ],
                'file_size': u'2012211901', 'auto_cross_section': 0.0,
                'check_sum': u'1504266448',
                'file_lumi_list': [
                    {'lumi_section_num': 27414+i, 'run_num': 98, 'event_count': 66},
                    {'lumi_section_num': 26422+i, 'run_num': 98, 'event_count': 67},
                    {'lumi_section_num': 29838+i, 'run_num': 98, 'event_count': 68},
                    ],
                'event_count': u'201',
                'logical_file_name': "/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/StepChain_/p%s/%i.root" %(uid, i),
                #'is_file_valid': 1
                }

            pflist.append(f)
            pFileAlgo = algo.copy()
            pFileAlgo['lfn'] = f['logical_file_name']
            parent_data['file_conf_list'].append(pFileAlgo)
            parent_stepchain_files.append(f['logical_file_name'])
            cf = f.copy()
            cf.update({'logical_file_name': "/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM/StepChain_/%s/%i.root" % (uid, i)})
            cflist.append(cf)
            cFileAlgo = algo.copy()
            cFileAlgo['lfn'] = cf['logical_file_name']
            stepchain_files.append(cf['logical_file_name'])
            data['file_conf_list'].append(cFileAlgo)

        parent_data["files"] = pflist
        data["files"] = cflist

        #self.api.insertBulkBlock(blockDump=parent_data)
        print("parent dataset: %s", parent_data['dataset']['dataset'], parent_data['primds']['primary_ds_name'] ,parent_data['dataset']['processed_ds_name'] ,parent_data['dataset']['data_tier_name'])
        print("parent block: %s", parent_data['block']['block_name'])
        print("parent files: ", len(parent_data["files"]))
        print("child dataset: %s", data['dataset']['dataset'],data['primds']['primary_ds_name'] , data['dataset']['processed_ds_name'] ,data['dataset']['data_tier_name'] )
        print("child block %s", data['block']['block_name'])
        print("child files: ", len(data["files"]))
        self.api.insertBulkBlock(blockDump=parent_data)
        self.api.insertBulkBlock(blockDump=data)
        #print("child dataset: %s", data['dataset']['dataset'])
        #print("child block %s", data['block']['block_name'])
        #print("child files: ", len(data["files"]))

    def test24(self):
        """test24 web.DBSClientWriter.insertFileParents: integration test validating the results"""
        result = self.api.listFileParentsByLumi(block_name=stepchain_block)
        print(result)
        child_parent_ids = result[0]["child_parent_id_list"]
        self.api.insertFileParents({"block_name": stepchain_block, "child_parent_id_list": child_parent_ids})
        file_name_pair = self.api.listFileParents(block_name=stepchain_block)
        parentIDs = set()
        for cpPair in child_parent_ids:
            parentIDs.add(cpPair[1])

        result2 = self.api.listFileParentsByLumi(block_name=stepchain_block)

        #compair the call whether there listFileParentsByLumi returns the same result after the insert
        self.assertEqual(result, result2)

        print(file_name_pair)
        # compare child parent pair.
        idPair = []
        for fInfo in file_name_pair:
            childFile = fInfo['logical_file_name']
            parentFile = fInfo['parent_logical_file_name'][0]
            index = stepchain_files.index(childFile)
            self.assertEqual(parent_stepchain_files[index], parentFile)
            cfDetail = self.api.listFiles(logical_file_name=childFile, detail=True)[0]
            ffDetail = self.api.listFiles(logical_file_name=parentFile, detail=True)[0]
            idPair.append([cfDetail["file_id"], ffDetail["file_id"]])

        self.assertEqual(len(child_parent_ids), len(file_name_pair))

        for ids in child_parent_ids:
            for nids in idPair:
                if ids[0] == nids[0]:
                    self.assertEqual(ids[1], nids[1])

    def test25(self):
        """test25 web.DBSClientWriter.insertFileParents: negtive test"""
        with self.assertRaises(HTTPError):
            self.api.insertFileParents(fileParentObj={"block_name": stepchain_block})

    def test208(self):
        """test208 generating the output file for reader test"""
        infoout=open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "info.dict"), "w")
        infoout.write("info="+str(outDict))
        infoout.close()

if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSClientWriter_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
