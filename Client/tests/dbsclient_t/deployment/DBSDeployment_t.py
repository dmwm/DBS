"""
DBS 3 Post-Deployment Tests for Operators of CMSWEB (These tests are read only!)
"""
import json
import os
import unittest
from dbs.apis.dbsClient import *
from dbs.exceptions.dbsClientException import dbsClientException

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
        expected_data = json.load(fp)
        
        acquisitioneras = self.api.listAcquisitionEras(acquisition_era_name="Commissioning11")
        self.assertEqual(expected_data,acquisitioneras)

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
        fp = file("BlockChildren.json",'r')
        expected_data = sorted(json.load(fp), key=lambda k: k["block_name"])
        
        blockchildren = sorted(self.api.listBlockChildren(block_name="/JetMETTau/Run2010A-v1/RAW#00513c84-93d1-4f71-81ea-440d9451861a"), key=lambda k: k["block_name"])

        self.assertEqual(expected_data,blockchildren)

        fp.close()

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
        fp = file("BlockParents.json",'r')
        expected_data = json.load(fp)
        
        blockparents = self.api.listBlockParents(block_name="/JetMETTau/Run2010A-PromptReco-v1/RECO#c737f664-42f3-49f5-8e3c-6f930eae6b0c")

        self.assertEqual(expected_data,blockparents)

        fp.close()
        
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
        expected_data = sorted(json.load(fp), key=lambda k: k["block_name"])
        
        blocks = sorted(self.api.listBlocks(dataset="/JetMETTau/Run2010A-v1/RAW"), key=lambda k: k["block_name"])

        self.assertEqual(expected_data,blocks)

        fp.close()

        expected_data = [{u'block_name': u'/JetMETTau/Run2010A-v1/RAW#8c0cf576-cf55-4379-8c47-dee34ee68c80'}]
        
        blocks = self.api.listBlocks(block_name="/JetMETTau/Run2010A-v1/RAW#8c0cf576-cf55-4379-8c47-dee34ee68c80")

        self.assertEqual(expected_data,blocks)

        blocks = self.api.listBlocks(logical_file_name="/store/data/Run2010A/JetMETTau/RAW/v1/000/136/063/3470B5EC-0866-DF11-ADD9-0030487CD906.root")

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
        
        datasets = self.api.listDatasets(dataset="/JetMETTau/Run2010A-v1/RAW",dataset_access_type="*",detail="True")

        self.assertEqual(expected_data,datasets)

        fp.close()

        datasets = self.api.listDatasets(dataset="/JetMETTau/Run2010A-v1/RAW",dataset_access_type="*",detail="True",data_tier_name="RAW")

        self.assertEqual(expected_data,datasets)

        datasets = self.api.listDatasets(dataset="/JetMETTau/Run2010A-v1/RAW",dataset_access_type="*",detail="True",primary_ds_name="JetMETTau")

        self.assertEqual(expected_data,datasets)

        datasets = self.api.listDatasets(dataset_access_type="*",detail="True", logical_file_name="/store/data/Run2010A/JetMETTau/RAW/v1/000/136/063/3470B5EC-0866-DF11-ADD9-0030487CD906.root")

        self.assertEqual(expected_data,datasets)

    def test_list_dataset_access_types(self):
        fp = file("DatasetAccessTypes.json",'r')
        expected_data = json.load(fp)
        
        datasetaccesstypes = self.api.listDatasetAccessTypes()

        self.assertEqual(expected_data,datasetaccesstypes)

        fp.close()

    def test_list_dataset_children(self):
        fp = file("DatasetChildren.json",'r')
        expected_data = sorted(json.load(fp), key=lambda k: k["child_dataset_id"])
        
        children = sorted(self.api.listDatasetChildren(dataset="/JetMETTau/Run2010A-v1/RAW"), key=lambda k: k["child_dataset_id"])

        self.assertEqual(expected_data,children)

        fp.close()

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
        fp = file("DatasetParents.json",'r')
        expected_data = json.load(fp)
        
        parents = self.api.listDatasetParents(dataset="/MinBias_TuneD6T_7TeV-pythia6/Summer10-START36_V10_SP10-v1/GEN-SIM-RECODEBUG")

        self.assertEqual(expected_data,parents)

        fp.close()

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
        expected_data = json.load(fp)

        datatiers = self.api.listDataTiers(data_tier_name="GEN-SIM-DIGI-HLTDEBUG-RECO")

        self.assertEqual(expected_data,datatiers)

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
        expected_data = json.load(fp)
    
        datatypes = self.api.listDataTypes(datatype="TEST")
        
        self.assertEqual(expected_data,datatypes)

        fp.close()

        datatypes = self.api.listDataTypes()

        result = []

        if len(datatypes)!=0:
            result.append(datatypes[0].has_key("primary_ds_type_id"))
            result.append(datatypes[0].has_key("data_type"))
        
        self.assertTrue(len(datatypes)!=0)
        self.assertFalse(False in result)

    def test_list_file_children(self):
        fp = file("FileChildren.json",'r')
        expected_data = json.load(fp)
        
        children = self.api.listFileChildren(logical_file_name="/store/data/Run2010A/JetMETTau/RAW/v1/000/136/063/3470B5EC-0866-DF11-ADD9-0030487CD906.root")

        self.assertEqual(expected_data,children)

        fp.close()
        
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

        lumis = sorted(self.api.listFileLumis(block_name="/JetMETTau/Run2010A-v1/RAW#8c0cf576-cf55-4379-8c47-dee34ee68c80"), key=lambda k: k["lumi_section_num"])

        self.assertEqual(expected_data,lumis)

        fp.close()

        fp = file("FileLumis2.json",'r')
        expected_data = sorted(json.load(fp), key=lambda k: k["lumi_section_num"])

        lumis = sorted(self.api.listFileLumis(logical_file_name="/store/data/Run2010A/JetMETTau/RAW/v1/000/136/063/3470B5EC-0866-DF11-ADD9-0030487CD906.root"), key=lambda k: k["lumi_section_num"])

        self.assertEqual(expected_data,lumis)

        fp.close()
        
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
        
        parents = sorted(self.api.listFileParents(block_name="/JetMETTau/Run2010A-Dec22ReReco_v1/AOD#fe7240b6-505d-4624-be0b-68f525697638"), key=lambda k: k["parent_logical_file_name"])

        self.assertEqual(expected_data,parents)

        fp.close()

        fp = file("FileParents2.json",'r')
        expected_data = sorted(json.load(fp), key=lambda k: k["parent_logical_file_name"])
        
        parents = sorted(self.api.listFileParents(logical_file_name="/store/data/Run2010A/JetMETTau/AOD/Dec22ReReco_v1/0088/AEA4A191-BC1C-E011-9912-003048678FD6.root"), key=lambda k: k["parent_logical_file_name"])
        
        self.assertEqual(expected_data,parents)

        fp.close()
        
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
        
        files = self.api.listFiles(logical_file_name="/store/data/Run2010A/JetMETTau/AOD/Dec22ReReco_v1/0088/AEA4A191-BC1C-E011-9912-003048678FD6.root",detail="True")

        self.assertEqual(expected_data,files)

        files = self.api.listFiles(dataset="/JetMETTau/Run2010A-Dec22ReReco_v1/AOD",logical_file_name="/store/data/Run2010A/JetMETTau/AOD/Dec22ReReco_v1/0088/AEA4A191-BC1C-E011-9912-003048678FD6.root",detail="True")

        self.assertEqual(expected_data,files)

        files = self.api.listFiles(block_name="/JetMETTau/Run2010A-Dec22ReReco_v1/AOD#fe7240b6-505d-4624-be0b-68f525697638",logical_file_name="/store/data/Run2010A/JetMETTau/AOD/Dec22ReReco_v1/0088/AEA4A191-BC1C-E011-9912-003048678FD6.root",detail="True")
        
        self.assertEqual(expected_data,files)

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
        fp = file("FileSummaries.json",'r')
        expected_data = json.load(fp)
        
        summaries = self.api.listFileSummaries(block_name="/JetMETTau/Run2010A-v1/RAW#8c0cf576-cf55-4379-8c47-dee34ee68c80")

        self.assertEqual(expected_data,summaries)

        fp.close()

        fp = file("FileSummaries2.json",'r')
        expected_data = json.load(fp)
        
        summaries = self.api.listFileSummaries(dataset="/JetMETTau/Run2010A-v1/RAW")

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
        expected_data = json.load(fp)
        
        configs = self.api.listOutputConfigs(dataset="/JetMETTau/Run2010A-v1/RAW",logical_file_name="/store/data/Run2010A/JetMETTau/RAW/v1/000/136/063/3470B5EC-0866-DF11-ADD9-0030487CD906.root",app_name="cmsRun")
        self.assertEqual(expected_data,configs)

        configs = self.api.listOutputConfigs(dataset="/JetMETTau/Run2010A-v1/RAW",logical_file_name="/store/data/Run2010A/JetMETTau/RAW/v1/000/136/063/3470B5EC-0866-DF11-ADD9-0030487CD906.root")
        self.assertEqual(expected_data,configs)
        
        configs = self.api.listOutputConfigs(logical_file_name="/store/data/Run2010A/JetMETTau/RAW/v1/000/136/063/3470B5EC-0866-DF11-ADD9-0030487CD906.root")
        self.assertEqual(expected_data,configs)

        fp.close()

    def test_list_physics_groups(self):
        fp = file("PhysicsGroups.json",'r')
        expected_data = sorted(json.load(fp), key=lambda k: k["physics_group_name"])
        
        groups = sorted(self.api.listPhysicsGroups(), key=lambda k: k["physics_group_name"])

        self.assertEqual(expected_data,groups)

        fp.close()

        groups = self.api.listPhysicsGroups(physics_group_name="susy")

        self.assertEqual('susy',groups[0].get('physics_group_name'))

    def test_list_primarydatasets(self):
        fp = file("PrimaryDatasets.json",'r')
        expected_data = sorted(json.load(fp), key=lambda k: k['primary_ds_id'])

        primarydatasets = sorted(self.api.listPrimaryDatasets(primary_ds_name="*Tau3Mu*"), key=lambda k: k['primary_ds_id'])

        self.assertEqual(expected_data,primarydatasets)

        fp.close()

    def test_list_processing_eras(self):
        fp = file("ProcessingEras.json",'r')
        expected_data = json.load(fp)
        
        eras = self.api.listProcessingEras(processing_version="4998")

        self.assertEqual(expected_data,eras)

        fp.close()

    def test_list_release_versions(self):
        fp = file("ReleaseVersions.json",'r')
        expected_data = json.load(fp)
        
        versions = self.api.listReleaseVersions(dataset="/JetMETTau/Run2010A-v1/RAW")

        self.assertEqual(expected_data,versions)

        fp.close()

        expected_data = [{u'release_version': [u'CMSSW_1_2_3']}]
        versions = self.api.listReleaseVersions(release_version="CMSSW_1_2_3")

        self.assertEqual(expected_data,versions)

    def test_list_runs(self):
        fp = file("RunList.json",'r')
        expected_data = json.load(fp)
        
        runs = self.api.listRuns(minrun=1095,maxrun=1100)
        
        self.assertEqual(expected_data,runs)

        fp.close()

    def test_help_page(self):
        fp = file("help.json",'r')
        expected_data = sorted(json.load(fp))

        help_page = sorted(self.api.help())

        self.assertEqual(expected_data,help_page)

        fp.close()

if __name__ == "__main__":
    RESTModel = ('DBSReader','DBSWriter')

    TestSuite = unittest.TestSuite()

    for model in RESTModel:
        loadedTests = unittest.TestLoader().loadTestsFromTestCase(PostDeploymentTests)

        for test in loadedTests:
            test.RESTModel = model

        TestSuite.addTests(loadedTests)
        
    unittest.TextTestRunner(verbosity=2).run(TestSuite)
