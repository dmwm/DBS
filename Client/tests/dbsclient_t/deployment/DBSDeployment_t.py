"""
DBS 3 Post-Deployment Tests for Operators of CMSWEB (These tests are read only!)
"""
import json
import os
import unittest
from dbs.apis.dbsClient import *
from dbs.exceptions.dbsClientException import dbsClientException

def stripChangingParameters(data):
    keys2remove = ['create_by','creation_date','data_tier_id','primary_ds_type_id','primary_ds_id',
                   'last_modified_by','last_modification_date','dataset_id','origin_site_name','child_dataset_id',
                   'parent_dataset_id', 'file_id', 'file_type_id', 'block_id', 'start_date', 'end_date']

    if isinstance(data,dict):
        for key in keys2remove:
            if data.has_key(key):
                del data[key]

    elif isinstance(data,list):
        data = [stripChangingParameters(entry) for entry in data]

    else:
        pass

    return data

class PrepareDeploymentsTests(unittest.TestCase):
    def setUp(self):
        self.url=os.environ['DBS_WRITER_URL']
        
        self.api = DbsApi(url=self.url)

    def test_01_insert_primary_dataset(self):
        fp = file("PrimaryDatasets.json",'r')

        data = json.load(fp)

        fp.close()
        
        self.api.insertPrimaryDataset(data)
        
    def test_02_insert_output_config(self):
        fp = file("OutputConfigs.json",'r')

        data = json.load(fp)

        fp.close()
        
        self.api.insertOutputConfig(data)
        
    def test_03_insert_acquisition_era(self):
        fp = file("Acquisitioneras.json",'r')

        data = json.load(fp)

        fp.close()

        self.api.insertAcquisitionEra(data)

    def test_04_insert_processing_era(self):
        fp = file("ProcessingEras.json",'r')

        data = json.load(fp)

        fp.close()

        self.api.insertProcessingEra(data)

    def test_05_insert_datatier(self):
        fp = file("DataTiers.json",'r')

        data = json.load(fp)

        fp.close()
        
        self.api.insertDataTier(data)

    def test_06_insert_dataset(self):
        fp = file("DatasetList.json",'r')

        data = json.load(fp)

        fp.close()
        
        self.api.insertDataset(data)

    def test_07_insert_child_dataset(self):
        fp = file("ChildDatasetList.json",'r')

        data = json.load(fp)

        fp.close()
        
        self.api.insertDataset(data)

    def test_08_insert_block(self):
        fp = file("BlockList.json",'r')

        data = json.load(fp)

        fp.close()

        for block in data:
            self.api.insertBlock(block)

    def test_09_insert_child_block(self):
        fp = file("ChildBlockList.json",'r')

        data = json.load(fp)

        fp.close()

        for block in data:
            self.api.insertBlock(block)

    def test_10_insert_files(self):
        fp = file("FileList.json",'r')

        data = json.load(fp)

        fp.close()

        ##spit data into chunks of 10 files, because DBS3 has a limitation of injecting max. 10 files in a chunk

        fileList = data['files']
        
        fileList = [fileList[i:i+10] for i  in range(0, len(fileList), 10)]

        for entry in fileList:
            self.api.insertFiles(filesList={'files':entry})
            

    def test_11_insert_child_files(self):
        fp = file("ChildFileList.json",'r')

        data = json.load(fp)

        fp.close()

        ##spit data into chunks of 10 files, because DBS3 has a limitation of injecting max. 10 files in a chunk

        fileList = data['files']
        
        fileList = [fileList[i:i+10] for i  in range(0, len(fileList), 10)]

        for entry in fileList:
            self.api.insertFiles(filesList={'files':entry})

class PostDeploymentTests(unittest.TestCase):
    def __init__(self, methodName='runTest'):
        self.RESTModel = 'DBSReader'
        super(PostDeploymentTests,self).__init__(methodName)
        
    def __str__(self):
        ''' Override this so that we know which instance it is '''
        return "(%s): %s (%s)" % (self.RESTModel, self._testMethodName, unittest._strclass(self.__class__))
        
    def setUp(self):
        if self.RESTModel == 'DBSReader':
            self.url=os.environ['DBS_READER_URL']
        else:
            self.url=os.environ['DBS_WRITER_URL']
            
        self.api = DbsApi(url=self.url)
            
    def test_list_acquisitioneras(self):
        fp = file("Acquisitioneras.json",'r')
        expected_data = [json.load(fp)]
        
        acquisitioneras = self.api.listAcquisitionEras(acquisition_era_name="DBS3_DEPLOYMENT_TEST_ERA")
        self.assertEqual(expected_data,stripChangingParameters(acquisitioneras))

        fp.close()

        acquisitioneras = self.api.listAcquisitionEras()

        result = []

        if len(acquisitioneras)!=0:
            result.append(acquisitioneras[0].has_key("acquisition_era_name"))
            result.append(acquisitioneras[0].has_key("create_by"))
            result.append(acquisitioneras[0].has_key("description"))
            result.append(acquisitioneras[0].has_key("creation_date"))

        self.assertTrue(len(acquisitioneras)!=0)
        self.assertFalse(False in result)

    def test_list_block_children(self):
        expected_data = [{u'block_name': u'/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST_CHILD-v4711/RECO#8c0cf576-cf55-4379-8c47-dee34ee68c81'}]
        
        blockchildren = sorted(self.api.listBlockChildren(block_name="/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST-v4711/RAW#8c0cf576-cf55-4379-8c47-dee34ee68c81"), key=lambda k: k["block_name"])

        self.assertEqual(expected_data,blockchildren)

        try:
            self.api.listBlockChildren()
        except dbsClientException:
            result = True
        except:
            result = False
        else:
            result = False
            
        self.assertTrue(result)

    def test_list_block_parents(self):
        expected_data = [{u'parent_block_name': u'/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST-v4711/RAW#8c0cf576-cf55-4379-8c47-dee34ee68c81', u'this_block_name': u'/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST_CHILD-v4711/RECO#8c0cf576-cf55-4379-8c47-dee34ee68c81'}]
        
        blockparents = self.api.listBlockParents(block_name="/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST_CHILD-v4711/RECO#8c0cf576-cf55-4379-8c47-dee34ee68c81")

        self.assertEqual(expected_data,blockparents)
        
        try:
            self.api.listBlockParents()
        except dbsClientException:
            result = True
        except:
            result = False
        else:
            result = False
            
        self.assertTrue(result)

    def test_list_blocks(self):
        fp = file("BlockList.json",'r')
        expected_data = stripChangingParameters(sorted(json.load(fp), key=lambda k: k["block_name"]))
        
        blocks = sorted(self.api.listBlocks(dataset="/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST-v4711/RAW"), key=lambda k: k["block_name"])

        self.assertEqual(expected_data,blocks)

        fp.close()

        expected_data = [{u'block_name': u'/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST-v4711/RAW#8c0cf576-cf55-4379-8c47-dee34ee68c81'}]
        
        blocks = self.api.listBlocks(block_name="/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST-v4711/RAW#8c0cf576-cf55-4379-8c47-dee34ee68c81")

        self.assertEqual(expected_data,blocks)

        blocks = self.api.listBlocks(logical_file_name="/store/mc/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST-v4711/RAW/DBS3_DEPLOYMENT_TEST/123456789/8c0cf576-cf55-4379-8c47-dee34ee68c81_0.root")

        self.assertEqual(expected_data,blocks)        

        try:
            self.api.listBlocks()
        except dbsClientException:
            result = True
        except:
            result = False
        else:
            result = False
            
        self.assertTrue(result)

    def test_list_datasets(self):
        fp = file("DatasetList.json",'r')
        expected_data = json.load(fp)

        del expected_data['output_configs']
        
        datasets = stripChangingParameters(self.api.listDatasets(dataset="/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST-v4711/RAW",dataset_access_type="*",detail="True"))

        self.assertEqual([expected_data],datasets)

        fp.close()

        datasets = stripChangingParameters(self.api.listDatasets(dataset="/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST-v4711/RAW",dataset_access_type="*",detail="True",data_tier_name="RAW"))

        self.assertEqual([expected_data],datasets)

        datasets = stripChangingParameters(self.api.listDatasets(dataset="/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST-v4711/RAW",dataset_access_type="*",detail="True",primary_ds_name="DBS3DeploymentTestPrimary"))

        self.assertEqual([expected_data],datasets)

        datasets = stripChangingParameters(self.api.listDatasets(dataset_access_type="*",detail="True", logical_file_name="/store/mc/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST-v4711/RAW/DBS3_DEPLOYMENT_TEST/123456789/8c0cf576-cf55-4379-8c47-dee34ee68c81_0.root"))

        self.assertEqual([expected_data],datasets)

    def test_list_dataset_access_types(self):
        fp = file("DatasetAccessTypes.json",'r')
        expected_data = json.load(fp)
        expected_data[0]["dataset_access_type"].sort()
        
        datasetaccesstypes = self.api.listDatasetAccessTypes()
        datasetaccesstypes[0]["dataset_access_type"].sort()

        self.assertEqual(expected_data,datasetaccesstypes)

        fp.close()

    def test_list_dataset_children(self):
        expected_data = [{u'child_dataset': u'/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST_CHILD-v4711/RECO', u'dataset': u'/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST-v4711/RAW'}]
        
        children = stripChangingParameters(self.api.listDatasetChildren(dataset="/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST-v4711/RAW"))

        self.assertEqual(expected_data,children)

        try:
            self.api.listDatasetChildren()
        except dbsClientException:
            result = True
        except:
            result = False
        else:
            result = False
            
        self.assertTrue(result)

    def test_list_dataset_parents(self):
        expected_data = [{u'this_dataset': u'/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST_CHILD-v4711/RECO', u'parent_dataset': u'/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST-v4711/RAW'}]
        
        parents = stripChangingParameters(self.api.listDatasetParents(dataset="/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST_CHILD-v4711/RECO"))

        self.assertEqual(expected_data,parents)

        try:
            self.api.listDatasetParents()
        except dbsClientException:
            result = True
        except:
            result = False
        else:
            result = False
            
        self.assertTrue(result)
                    
    def test_list_datatiers(self):
        fp = file("DataTiers.json",'r')
        expected_data = [json.load(fp)]

        datatiers = self.api.listDataTiers(data_tier_name="DBS3_DEPLOYMENT_TEST_TIER")

        self.assertEqual(expected_data,stripChangingParameters(datatiers))

        fp.close()

        datatiers = self.api.listDataTiers()

        result = []

        if len(datatiers)!=0:
            result.append(datatiers[0].has_key("create_by"))
            result.append(datatiers[0].has_key("creation_date"))
            result.append(datatiers[0].has_key("data_tier_name"))
            result.append(datatiers[0].has_key("data_tier_id"))

        self.assertTrue(len(datatiers)!=0)
        self.assertFalse(False in result)

    def test_list_datatypes(self):
        fp = file("DataTypes.json",'r')
        expected_data = [json.load(fp)]
    
        datatypes = self.api.listDataTypes(datatype="TEST")
        
        self.assertEqual(expected_data,stripChangingParameters(datatypes))

        fp.close()

        datatypes = self.api.listDataTypes()

        result = []

        if len(datatypes)!=0:
            result.append(datatypes[0].has_key("primary_ds_type_id"))
            result.append(datatypes[0].has_key("data_type"))
        
        self.assertTrue(len(datatypes)!=0)
        self.assertFalse(False in result)

    def test_list_file_children(self):
        expected_data = [{u'child_logical_file_name': [u'/store/mc/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST_CHILD-v4711/RECO/DBS3_DEPLOYMENT_TEST/123456789/8c0cf576-cf55-4379-8c47-dee34ee68c81_0.root'], u'logical_file_name': u'/store/mc/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST-v4711/RAW/DBS3_DEPLOYMENT_TEST/123456789/8c0cf576-cf55-4379-8c47-dee34ee68c81_0.root'}]
        
        children = self.api.listFileChildren(logical_file_name="/store/mc/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST-v4711/RAW/DBS3_DEPLOYMENT_TEST/123456789/8c0cf576-cf55-4379-8c47-dee34ee68c81_0.root")

        self.assertEqual(expected_data,children)

        try:
            self.api.listFileChildren()
        except dbsClientException:
            result = True
        except:
            result = False
        else:
            result = False
            
        self.assertTrue(result)

    def test_list_file_lumis(self):
        fp = file("FileLumis.json",'r')
        expected_data = sorted(json.load(fp), key=lambda k: k["lumi_section_num"])

        lumis = sorted(self.api.listFileLumis(block_name="/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST-v4711/RAW#8c0cf576-cf55-4379-8c47-dee34ee68c81"), key=lambda k: k["lumi_section_num"])

        self.assertEqual(expected_data,lumis)

        fp.close()

        expected_data = [{u'lumi_section_num': 24022, u'run_num': 43}, {u'lumi_section_num': 24122, u'run_num': 43}, {u'lumi_section_num': 24222, u'run_num': 43}]

        lumis = sorted(self.api.listFileLumis(logical_file_name="/store/mc/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST-v4711/RAW/DBS3_DEPLOYMENT_TEST/123456789/8c0cf576-cf55-4379-8c47-dee34ee68c81_0.root"), key=lambda k: k["lumi_section_num"])

        self.assertEqual(expected_data,lumis)

        try:
            self.api.listFileLumis()
        except dbsClientException:
            result = True
        except:
            result = False
        else:
            result = False
            
        self.assertTrue(result)

    def test_list_file_parents(self):
        fp = file("FileParents.json",'r')
        expected_data = sorted(json.load(fp), key=lambda k: k["parent_logical_file_name"])
        
        parents = sorted(self.api.listFileParents(block_name="/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST_CHILD-v4711/RECO#8c0cf576-cf55-4379-8c47-dee34ee68c81"), key=lambda k: k["parent_logical_file_name"])

        self.assertEqual(expected_data,parents)

        fp.close()

        expected_data = [{u'parent_logical_file_name': [u'/store/mc/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST-v4711/RAW/DBS3_DEPLOYMENT_TEST/123456789/8c0cf576-cf55-4379-8c47-dee34ee68c81_0.root'], u'logical_file_name': u'/store/mc/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST_CHILD-v4711/RECO/DBS3_DEPLOYMENT_TEST/123456789/8c0cf576-cf55-4379-8c47-dee34ee68c81_0.root'}]
        
        parents = self.api.listFileParents(logical_file_name="/store/mc/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST_CHILD-v4711/RECO/DBS3_DEPLOYMENT_TEST/123456789/8c0cf576-cf55-4379-8c47-dee34ee68c81_0.root")
        
        self.assertEqual(expected_data,parents)

        try:
            self.api.listFileParents()
        except dbsClientException:
            result = True
        except:
            result = False
        else:
            result = False
            
        self.assertTrue(result)

    def test_list_files(self):
        fp = file("ListFiles.json",'r')
        expected_data = json.load(fp)
        
        files = self.api.listFiles(logical_file_name="/store/mc/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST_CHILD-v4711/RECO/DBS3_DEPLOYMENT_TEST/123456789/8c0cf576-cf55-4379-8c47-dee34ee68c81_0.root",detail="True")
        

        self.assertEqual(expected_data,stripChangingParameters(files))

        files = self.api.listFiles(dataset="/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST_CHILD-v4711/RECO",logical_file_name="/store/mc/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST_CHILD-v4711/RECO/DBS3_DEPLOYMENT_TEST/123456789/8c0cf576-cf55-4379-8c47-dee34ee68c81_0.root",detail="True")

        self.assertEqual(expected_data,stripChangingParameters(files))

        files = self.api.listFiles(block_name="/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST_CHILD-v4711/RECO#8c0cf576-cf55-4379-8c47-dee34ee68c81",logical_file_name="/store/mc/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST_CHILD-v4711/RECO/DBS3_DEPLOYMENT_TEST/123456789/8c0cf576-cf55-4379-8c47-dee34ee68c81_0.root",detail="True")
        
        self.assertEqual(expected_data,stripChangingParameters(files))

        fp.close()

        try:
            self.api.listFiles()
        except dbsClientException:
            result = True
        except:
            result = False
        else:
            result = False
            
        self.assertTrue(result)

    def test_list_file_summaries(self):
        expected_data = [{u'num_block': 1, u'num_file': 10, u'num_event': 553964, u'num_lumi': 30, u'file_size': 25350778463}]
        
        summaries = self.api.listFileSummaries(block_name="/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST-v4711/RAW#8c0cf576-cf55-4379-8c47-dee34ee68c81")

        self.assertEqual(expected_data,summaries)

        expected_data = [{u'num_block': 1, u'num_file': 10, u'num_event': 553964, u'num_lumi': 30, u'file_size': 25350778463}]
        
        self.api.listFileSummaries(dataset="/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST-v4711/RAW")

        self.assertEqual(expected_data,summaries)

        try:
            self.api.listFileSummaries()
        except dbsClientException:
            result = True
        except:
            result = False
        else:
            result = False
            
        self.assertTrue(result)

    def test_list_output_configs(self):
        fp = file("OutputConfigs.json",'r')
        expected_data = [json.load(fp)]
        
        configs = self.api.listOutputConfigs(dataset="/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST-v4711/RAW",logical_file_name="/store/mc/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST-v4711/RAW/DBS3_DEPLOYMENT_TEST/123456789/8c0cf576-cf55-4379-8c47-dee34ee68c81_0.root",app_name="cmsRun")
        self.assertEqual(expected_data,configs)

        configs = self.api.listOutputConfigs(dataset="/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST-v4711/RAW",logical_file_name="/store/mc/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST-v4711/RAW/DBS3_DEPLOYMENT_TEST/123456789/8c0cf576-cf55-4379-8c47-dee34ee68c81_0.root")
        self.assertEqual(expected_data,configs)
        
        configs = self.api.listOutputConfigs(logical_file_name="/store/mc/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST-v4711/RAW/DBS3_DEPLOYMENT_TEST/123456789/8c0cf576-cf55-4379-8c47-dee34ee68c81_0.root")
        self.assertEqual(expected_data,configs)

        fp.close()

    def test_list_physics_groups(self):
        
        self.api.listPhysicsGroups()

        groups = self.api.listPhysicsGroups(physics_group_name="Top")

        self.assertEqual('Top',groups[0].get('physics_group_name'))

    def test_list_primarydatasets(self):
        fp = file("PrimaryDatasets.json",'r')
        expected_data = [json.load(fp)]

        primarydatasets = stripChangingParameters(self.api.listPrimaryDatasets(primary_ds_name="DBS3Deployment*"))

        self.assertEqual(expected_data,primarydatasets)

        fp.close()

    def test_list_processing_eras(self):
        fp = file("ProcessingEras.json",'r')
        expected_data = [json.load(fp)]
        
        eras = stripChangingParameters(self.api.listProcessingEras(processing_version="4711"))

        self.assertEqual(expected_data,eras)

        fp.close()

    def test_list_release_versions(self):
        fp = file("ReleaseVersions.json",'r')
        expected_data = json.load(fp)
        
        versions = self.api.listReleaseVersions(dataset="/DBS3DeploymentTestPrimary/DBS3_DEPLOYMENT_TEST_ERA-DBS3_DEPLOYMENT_TEST-v4711/RAW")

        self.assertEqual(expected_data,versions)

        fp.close()

        expected_data = [{u'release_version': [u'CMSSW_1_2_3']}]
        versions = self.api.listReleaseVersions(release_version="CMSSW_1_2_3")

        self.assertEqual(expected_data,versions)

    def test_list_runs(self):
        fp = file("RunList.json",'r')
        expected_data = json.load(fp)
        
        runs = self.api.listRuns(minrun=43,maxrun=43)
        
        self.assertEqual(expected_data,runs)

        fp.close()

    def test_help_page(self):
        fp = file("help.json",'r')
        expected_data = sorted(json.load(fp))

        help_page = sorted(self.api.help())

        self.assertEqual(expected_data,help_page)

        fp.close()

if __name__ == "__main__":
    import sys

    message =  "Usage:   python DBSDeployment_t.py insert=True \n\
                or python DBSDeployment_t.py insert=False "
    
    args = sys.argv
    if len(args) == 1:
        print message
        sys.exit()
    elif len(args) != 2:
        print message
        sys.exit()
    else:
        if args[1] not in ['insert=True', 'insert=False']: 
            print message
            sys.exit()
        
    
    RESTModel = ('DBSReader','DBSWriter')

    TestSuite = unittest.TestSuite()

    if args[1] == 'insert=True': 
        prepareTests = unittest.TestLoader().loadTestsFromTestCase(PrepareDeploymentsTests)
        TestSuite.addTests(prepareTests)
    else:
        pass
    for model in RESTModel:
        loadedTests = unittest.TestLoader().loadTestsFromTestCase(PostDeploymentTests)

        for test in loadedTests:
            test.RESTModel = model

        TestSuite.addTests(loadedTests)
        
    unittest.TextTestRunner(verbosity=2).run(TestSuite)
