"""
DBS3 Validation tests
These tests write and then immediately reads back the data from DBS3 and validate
"""
import os
import unittest
from dbsclient_t.utils.DBSDataProvider import DBSDataProvider
from dbsclient_t.utils.timeout import Timeout
from dbs.exceptions.dbsClientException import dbsClientException
from dbs.apis.dbsClient import *
import uuid
from random import choice

uid = uuid.uuid4().time_mid
print "****uid=%s******" % uid
acquisition_era_name = "acq_era_%s" % uid
processing_version = (uid if (uid < 9999) else uid % 9999)
primary_ds_name = 'unittest_web_primary_ds_name_%s' % uid
procdataset = '%s-unittest_web_dataset-v%s' % (acquisition_era_name, processing_version)
procdataset_parent = "%s-unittest_web_dataset_parent-v%s" % (acquisition_era_name, processing_version)
tier = 'GEN-SIM-RAW'
dataset = "/%s/%s/%s" % (primary_ds_name, procdataset, tier)
app_name = 'cmsRun%s' % uid
output_module_label = 'Merged'
global_tag = 'dbs-client-validation-%s' % uid
pset_hash = '76e303993a1c2f842159dbfeeed9a0dd%s' % uid
release_version = 'CMSSW_1_2_3%s' % uid
site = "cmssrm.fnal.gov"
block = "%s#%s" % (dataset, uid)

flist = []

def remove_non_comparable_keys(values, non_comparable_keys):
    for value in values:
        if isinstance(value, dict):
            keys = set(value.iterkeys())
            intersection = keys.intersection(set(non_comparable_keys))
            for entry in intersection:
                del value[entry]
        yield value

class DBSValidation_t(unittest.TestCase):
    def __init__(self, methodName='runTest'):
        super(DBSValidation_t, self).__init__(methodName)
        if not hasattr(self, 'data_provider'):
            self.setUpClass()
        url = os.environ['DBS_WRITER_URL']
        proxy = os.environ.get('SOCKS5_PROXY')
        self.api = DbsApi(url=url, proxy=proxy)

        migration_url = os.environ['DBS_MIGRATE_URL']
        self.migration_api = DbsApi(url=migration_url, proxy=proxy)

        self.cmsweb_api = DbsApi(url='https://cmsweb.cern.ch/dbs/prod/global/DBSReader', proxy=proxy)

    def setUp(self):
        """setup all necessary parameters"""

    @classmethod
    def setUpClass(cls):
        """Class method to set-up the class"""
        ### necessary since one instance per test case is created and pid and testparams
        ### need to be shared between instances
        cls.data_provider = DBSDataProvider()

    def test01(self):
        """test01: web.DBSClientWriter.PrimaryDataset: validation test"""
        data = {'primary_ds_name':primary_ds_name,
                'primary_ds_type':'test'}
        #print "data=%s" %data
        self.api.insertPrimaryDataset(primaryDSObj=data)
        primaryList = self.api.listPrimaryDatasets(primary_ds_name=primary_ds_name)
        self.assertEqual(len(primaryList), 1)
        primaryInDBS=primaryList[0]
        self.assertEqual(primaryInDBS['primary_ds_name'], primary_ds_name)
        self.assertEqual(primaryInDBS['primary_ds_type'], 'test')

    def test02(self):
        """test02: web.DBSClientWriter.OutputModule: validation test"""
        data = {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                'output_module_label': output_module_label, 'global_tag':global_tag}
        self.api.insertOutputConfig(outputConfigObj=data)
        confList=self.api.listOutputConfigs(release_version=release_version, pset_hash=pset_hash,
                                            app_name=app_name, output_module_label=output_module_label,
                                            global_tag=global_tag)
        self.assertEqual(len(confList), 1)
        confInDBS=confList[0]
        self.assertEqual(confInDBS['release_version'], release_version)
        self.assertEqual(confInDBS['pset_hash'], pset_hash)
        self.assertEqual(confInDBS['app_name'], app_name)
        self.assertEqual(confInDBS['output_module_label'], output_module_label)
        self.assertEqual(confInDBS['global_tag'], global_tag)

    def test03(self):
        """test03: web.DBSWriterModel.insertAcquisitionEra: Basic test """
        data={'acquisition_era_name': acquisition_era_name}
        self.api.insertAcquisitionEra(data)

    def test04(self):
        """test04: web.DBSWriterModel.insertProcessingEra: Basic test """
        data={'processing_version': processing_version, 'description':'this_is_a_test'}
        self.api.insertProcessingEra(data)

    def test05(self):
        """test05: web.DBSClientWriter.Dataset: validation test"""
        data = {
            'physics_group_name': 'Tracker', 'dataset': dataset,
            'dataset_access_type': 'PRODUCTION', 'processed_ds_name': procdataset, 'primary_ds_name': primary_ds_name,
            'output_configs': [
                {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name, \
                 'output_module_label': output_module_label, 'global_tag' : global_tag},
                ],
            'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': tier,
            'creation_date' : 1234, 'create_by' : 'anzar', "last_modification_date" : 1234, "last_modified_by" : "anzar",
            'processing_version': processing_version,  'acquisition_era_name': acquisition_era_name,
            }
        self.api.insertDataset(datasetObj=data)
        dsList = self.api.listDatasets(dataset=dataset, detail=True, dataset_access_type='PRODUCTION')

        self.assertEqual(len(dsList), 1)
        dsInDBS=dsList[0]
        #store create_by and last_modified_by information
        create_by = dsInDBS['create_by']
        last_modified_by = dsInDBS['last_modified_by']
        self.assertEqual(dsInDBS['dataset'], dataset)
        self.assertEqual(dsInDBS['physics_group_name'], 'Tracker')
        self.assertEqual(dsInDBS['dataset_access_type'], 'PRODUCTION')
        self.assertEqual(dsInDBS['processed_ds_name'], procdataset)
        self.assertEqual(dsInDBS['primary_ds_name'], primary_ds_name)
        self.assertEqual(dsInDBS['primary_ds_type'], 'test')
        self.assertEqual(dsInDBS['data_tier_name'], tier)
        self.assertEqual(dsInDBS['xtcrosssection'], 123)
        self.assertEqual(dsInDBS['processing_version'], processing_version)
        self.assertEqual(dsInDBS['acquisition_era_name'], acquisition_era_name)
        self.assertEqual(dsInDBS['prep_id'], None)

        dsList = self.api.listDatasets(dataset=dataset, detail=True, create_by=create_by, last_modified_by=last_modified_by, dataset_access_type='PRODUCTION')

        self.assertEqual(len(dsList), 1)
        dsInDBS=dsList[0]

        self.assertEqual(dsInDBS['dataset'], dataset)
        self.assertEqual(dsInDBS['physics_group_name'], 'Tracker')
        self.assertEqual(dsInDBS['dataset_access_type'], 'PRODUCTION')
        self.assertEqual(dsInDBS['processed_ds_name'], procdataset)
        self.assertEqual(dsInDBS['primary_ds_name'], primary_ds_name)
        self.assertEqual(dsInDBS['primary_ds_type'], 'test')
        self.assertEqual(dsInDBS['data_tier_name'], tier)
        self.assertEqual(dsInDBS['xtcrosssection'], 123)
        self.assertEqual(dsInDBS['processing_version'], processing_version)
        self.assertEqual(dsInDBS['acquisition_era_name'], acquisition_era_name)
        self.assertEqual(dsInDBS['prep_id'], None)

    def test06(self):
        """test06 web.DBSClientWriter.Block: validation test"""
        data = {'block_name': block,
                'origin_site_name': site }

        self.api.insertBlock(blockObj=data)
        blkList = self.api.listBlocks(block_name=block, detail=True)
        self.assertEqual(len(blkList), 1)
        blkInDBS=blkList[0]
        self.assertEqual(blkInDBS['origin_site_name'], site )
        self.assertEqual(blkInDBS['open_for_writing'], 1)
        self.assertEqual(blkInDBS['dataset'], dataset)
        self.assertEqual(blkInDBS['block_name'], block)
        self.assertEqual(blkInDBS['file_count'], 0)
        self.assertEqual(blkInDBS['block_size'], 0)

    def test07(self):
        """test07 web.DBSClientWriter.Files: validation test"""
        #
        #    --- NOTICE    _parent    at multiple places below
        # This first part just inserts a parent primary, dataset and block, with parent files
        # That is later used in inserting files in 'block', that are then 'validated'
        pridata = {'primary_ds_name':primary_ds_name+"_parent",
                   'primary_ds_type':'test'}
        self.api.insertPrimaryDataset(primaryDSObj=pridata)
        primary_ds_name_parent = primary_ds_name+'_parent'

        dataset_parent = "/%s/%s/%s" % (primary_ds_name_parent,procdataset_parent,tier)
        data = {
            'physics_group_name': 'Tracker', 'dataset': dataset_parent,
            'dataset_access_type': 'PRODUCTION', 'processed_ds_name': procdataset_parent,
            'primary_ds_name': primary_ds_name_parent,
            'output_configs': [
                {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                 'output_module_label': output_module_label, 'global_tag': global_tag},
                ],
            'xtcrosssection': 123, 'primary_ds_type': 'test', 'data_tier_name': tier,
            'creation_date' : 1234, 'create_by': 'anzar', "last_modification_date": 1234, "last_modified_by": "anzar",
            'processing_version': processing_version,  'acquisition_era_name': acquisition_era_name,
            }
        self.api.insertDataset(datasetObj=data)

        block_parent="/%s/%s/%s#%s" % (primary_ds_name_parent, procdataset_parent, tier, uid)
        pblkdata = {'block_name': block_parent,
                    'origin_site_name': site }
        self.api.insertBlock(blockObj=pblkdata)
        #parent files
        pflist=[]
        for i in range(10):
            f={
                'adler32': u'NOTSET', 'file_type': 'EDM',
                'dataset': dataset_parent,
                'file_size': u'201221191', 'auto_cross_section': 0.0,
                'check_sum': u'1504266448',
                'event_count': u'1619',
                'logical_file_name': "/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/%s/parent_%i.root" %(uid, i),
                'block_name': block_parent,
                'file_lumi_list': [
                    {'lumi_section_num': u'27414', 'run_num': u'1'},
                    {'lumi_section_num': u'26422', 'run_num': u'1'},
                    {'lumi_section_num': u'29838', 'run_num': u'1'}
                    ]
                }
            pflist.append(f)
        self.api.insertFiles(filesList={"files":pflist}, qInserts=False)
        #### This next block of test will now actually insert the files in the "test 'block' in this module, using the upper files as parent
        for i in range(10):
            f={
                'adler32': u'NOTSET', 'file_type': 'EDM',
                'file_output_config_list':
                [
                    {'release_version': release_version, 'pset_hash': pset_hash, 'app_name': app_name,
                     'output_module_label': output_module_label, 'global_tag': global_tag},
                    ],
                'dataset': dataset,
                'file_size': u'201221191', 'auto_cross_section': 0.0,
                'check_sum': u'1504266448',
                'file_lumi_list': [
                    {'lumi_section_num': u'27414', 'run_num': u'1'},
                    {'lumi_section_num': u'26422', 'run_num': u'1'},
                    {'lumi_section_num': u'29838', 'run_num': u'1'}
                    ],
                'file_parent_list': [ {"file_parent_lfn" : "/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/%s/parent_%i.root" %(uid, i)} ],
                'event_count': u'1619',
                'logical_file_name': "/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/%s/%i.root" %(uid, i),
                'block_name': block
                #'is_file_valid': 1
                }
            flist.append(f)

        self.api.insertFiles(filesList={"files":flist}, qInserts=False)

        ### Lets begin the validation now
        # our block, 'block' now has these 10 files, and that is basis of our validation
        flList=self.api.listFiles(block_name=block, detail=True)
        self.assertEqual(len(flList), len(flist))
        for afileInDBS in flList:
            self.assertEqual(afileInDBS['block_name'], block)
            self.assertEqual(afileInDBS['event_count'], 1619)
            self.assertEqual(afileInDBS['file_size'], 201221191)
            self.assertEqual(afileInDBS['is_file_valid'], 1)
        # Get the file parent -- The inserted file must have a parent
        logical_file_name = "/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/%s/%i.root" %(uid, 0)
        flParentList=self.api.listFileParents(logical_file_name=logical_file_name)
        self.assertEqual(len(flParentList), 1)
        self.assertEqual(flParentList[0]['parent_logical_file_name'][0], "/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/%s/parent_%i.root" %(uid, 0))
        logical_file_names = ["/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/%s/%i.root" %(uid, i) for i in xrange(10)]
        flParentList=self.api.listFileParents(logical_file_name=logical_file_names)
        self.assertEqual(len(flParentList), 10)
        self.assertEqual(sorted((flParentList[i]['parent_logical_file_name'][0] for i in xrange(10))),
                         sorted(("/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/%s/parent_%i.root" % (uid, i)) for i in xrange(10)))
        # Get the dataset parent -- due to fact that files had parents, dataset parentage is also inserted
        dsParentList=self.api.listDatasetParents(dataset=dataset)
        self.assertEqual(len(dsParentList), 1)
        self.assertEqual(dsParentList[0]['parent_dataset'], "/%s/%s/%s" % (primary_ds_name+"_parent", procdataset_parent, tier))
        # block parameters, such as file_count must also be updated, lets validate
        blkList = self.api.listBlocks(block_name=block, detail=True)
        self.assertEqual(len(blkList), 1)
        blkInDBS=blkList[0]
        self.assertEqual(blkInDBS['origin_site_name'], site )
        self.assertEqual(blkInDBS['open_for_writing'], 1)
        self.assertEqual(blkInDBS['dataset'], dataset)
        self.assertEqual(blkInDBS['block_name'], block)
        # 10 files
        self.assertEqual(blkInDBS['file_count'], 10)
        # size should be 10 X 2012211901 (file_size) = 2012211910
        self.assertEqual(blkInDBS['block_size'], 2012211910)

    def test08(self):
        """update file status and validate that it got updated"""
        logical_file_name = "/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/%s/%i.root" %(uid, 0)
        #print "WARNING : DBS cannot list INVALID file, so for now this test is commented out"
        self.api.updateFileStatus(logical_file_name=logical_file_name, is_file_valid=0)
        #listfile
        filesInDBS=self.api.listFiles(logical_file_name=logical_file_name, detail=True)
        self.assertEqual(len(filesInDBS) ,1)
        self.assertEqual(filesInDBS[0]['is_file_valid'], 0)

    def test09(self):
        """test09 web.DBSClientWriter.updateDatasetStatus: should be able to update dataset status and validate it"""
        #self.api.updateDatasetStatus(dataset=dataset, is_dataset_valid=1)
        dsInDBS=self.api.listDatasets(dataset=dataset,  dataset_access_type="PRODUCTION", detail=True)
        self.assertEqual(len(dsInDBS), 1)
        #self.assertEqual(dsInDBS[0]['is_dataset_valid'], 1)

    def test10(self):
        """test10 web.DBSClientWriter.updateDatasetType: should be able to update dataset type"""
        self.api.updateDatasetType(dataset=dataset, dataset_access_type="VALID")
        dsInDBS=self.api.listDatasets(dataset=dataset, detail=True)
        self.assertEqual(len(dsInDBS), 1)
        self.assertEqual(dsInDBS[0]['dataset_access_type'], "VALID")

    def test11(self):
        """test11 web.DBSClientWriter.insertBulkBlock"""
        input_block_dump = self.data_provider.block_dump()[0]
        self.api.insertBulkBlock(input_block_dump)
        block_dump = self.api.blockDump(block_name=input_block_dump['block']['block_name'])

        def check(input, output):
            if isinstance(input, dict):
                for key, value in input.iteritems():
                    self.assertTrue(output.has_key(key))
                    check(value, output[key])
            elif isinstance(input, list):
                for element_in, element_out in zip(sorted(input), sorted(output)):
                    check(element_in, element_out)
            else:
                self.assertEqual(str(input), str(output))

        check(input_block_dump, block_dump)

    def test12(self):
        """test12 Migration of datasets"""
        ###get a dataset to migrate from global dbs
        dest_datasets = set((dataset['dataset'] for dataset in self.api.listDatasets()))
        ###only dataset after last DBS2->3 because of the parentage issue in DBS 2 min_cdate=1368162000 =10May2013
        src_datasets = set((dataset['dataset'] for dataset in self.cmsweb_api.listDatasets(min_cdate=1368162000)))
        dataset_to_migrate = choice(list(src_datasets.difference(dest_datasets)))

        ###submit migration request
        toMigrate = {'migration_url': 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader',
                     'migration_input': dataset_to_migrate}
        migration_request = self.migration_api.submitMigration(toMigrate)
        self.assertTrue(migration_request['migration_details'].has_key('migration_request_id'))
        migration_request_id = migration_request['migration_details']['migration_request_id']

        ###check migration status for max. 300s (should be enough time to migrate the dataset)
        with Timeout(300):
            while True:
                request_status = self.migration_api.statusMigration(migration_rqst_id=migration_request_id)
                if request_status[0]['migration_status'] == 2:
                    break

        ###validate dataset migration
        def check(input, output):
            non_comparable_keys = ('block_id', 'dataset_id', 'last_modification_date',
                                   'parent_file_id', 'primary_ds_id')
            if isinstance(input, dict):
                for key, value in input.iteritems():
                    if key in non_comparable_keys:
                        continue ###do not compare id's
                    self.assertTrue(output.has_key(key))
                    check(value, output[key])
            elif isinstance(input, list):
                for element_in, element_out in zip(sorted(remove_non_comparable_keys(input, non_comparable_keys)),
                                                   sorted(remove_non_comparable_keys(output, non_comparable_keys))):
                    check(element_in, element_out)
            else:
                self.assertEqual(str(input), str(output))

        for block_name in (block['block_name'] for block in self.cmsweb_api.listBlocks(dataset=dataset_to_migrate)):
            block_dump_src = self.cmsweb_api.blockDump(block_name=block_name)
            block_dump_dest = self.api.blockDump(block_name=block_name)
            check(block_dump_src, block_dump_dest)

        ###try to delete successfully executed migration request
        toDelete = {'migration_rqst_id': migration_request_id}
        self.assertRaises(dbsClientException, self.migration_api.removeMigration, toDelete)

    def test13(self):
        """test13 Migration of blocks"""
        ###get a block to migrate from global dbs
        dest_datasets = set((dataset['dataset'] for dataset in self.api.listDatasets()))
        ###only dataset after last DBS2->3 because of the parentage issue in DBS 2 min_cdate=1368162000 =10May2013
        src_datasets = set((dataset['dataset'] for dataset in self.cmsweb_api.listDatasets(min_cdate=1368162000)))
        dataset_to_migrate = choice(list(src_datasets.difference(dest_datasets)))
        block_to_migrate = choice([block['block_name']
                                   for block in self.cmsweb_api.listBlocks(dataset=dataset_to_migrate)])

        ###submit migration request
        toMigrate = {'migration_url': 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader',
                     'migration_input': block_to_migrate}
        migration_request = self.migration_api.submitMigration(toMigrate)
        self.assertTrue(migration_request['migration_details'].has_key('migration_request_id'))
        migration_request_id = migration_request['migration_details']['migration_request_id']

        ###check migration status for max. 300s (should be enough time to migrate the dataset)
        with Timeout(300):
            while True:
                request_status = self.migration_api.statusMigration(migration_rqst_id=migration_request_id)
                if request_status[0]['migration_status'] == 2:
                    break

        ###validate block    migration
        def check(input, output):
            non_comparable_keys = ('block_id', 'dataset_id', 'last_modification_date',
                                   'parent_file_id', 'primary_ds_id')
            if isinstance(input, dict):
                for key, value in input.iteritems():
                    if key in non_comparable_keys:
                        continue ###do not compare id's
                    self.assertTrue(output.has_key(key))
                    check(value, output[key])
            elif isinstance(input, list):
                for element_in, element_out in zip(sorted(remove_non_comparable_keys(input, non_comparable_keys)),
                                                   sorted(remove_non_comparable_keys(output, non_comparable_keys))):
                    check(element_in, element_out)
            else:
                self.assertEqual(str(input), str(output))

        block_dump_src = self.cmsweb_api.blockDump(block_name=block_to_migrate)
        block_dump_dest = self.api.blockDump(block_name=block_to_migrate)
        check(block_dump_src, block_dump_dest)

        ###try to delete successfully executed migration request
        toDelete = {'migration_rqst_id': migration_request_id}
        self.assertRaises(dbsClientException, self.migration_api.removeMigration, toDelete)


if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSValidation_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
