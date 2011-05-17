"""
Unittests to validate the DBS2 to DBS3 migration
Author: Manuel Giffels <giffels@physik.rwth-aachen.de>
"""

import logging
import unittest

from DBS2SqlApi import DBS2SqlApi
from DBS3SqlApi import DBS3SqlApi

def listComparision(resultsDBS2,resultsDBS3):
    diffList = []

    if len(resultsDBS2)==len(resultsDBS3):

        #easiest test, lists are identical
        if resultsDBS2==resultsDBS3:
            return True
        
        #find the difference
        for resultDBS2 in resultsDBS2:
            if resultDBS2 not in resultsDBS3:
                print "No Match for: %s in DBS3" % (resultDBS2)
                diffList.append(resultDBS2)

        for resultDBS3 in resultsDBS3:
            if resultDBS3 not in resultsDBS2:
                print "No Match for: %s in DBS2" % (resultDBS3)
                diffList.append(resultDBS3)
        return False
            
    else: # first hint that something is wrong
        for resultDBS2 in resultsDBS2:
            if resultDBS2 not in resultsDBS3:
                print "No Match for: %s in DBS3" % (resultDBS2)
                diffList.append(resultDBS2)
                
        if len(diffList)==abs(len(resultsDBS2)-len(resultsDBS3)):
            return False
            
        else:
            for resultDBS3 in resultsDBS3:
                if resultDBS3 not in resultsDBS2:
                    print "No Match for: %s in DBS2" % (resultDBS3)
                    diffList.append(resultDBS3)
            return False

def diffKeys(resultDBS2,resultDBS3):
    for i in resultDBS3.keys():
        if resultDBS2.has_key(i):
            if resultDBS3[i]==resultDBS2[i]:
                pass
            else:
                print "%s differs: %s,%s" % (i,resultDBS2[i],resultDBS3[i])
        else:
            print "DBS2 has no key %s" % (i)

class CompareDBS2ToDBS3(unittest.TestCase):
    
    def setUp(self):
        ownerDBS3 = 'owner'
        connectUrlDBS3 = 'oracle://owner:passwd@instance'

        ownerDBS2 = 'owner'
        connectUrlDBS2 = 'oracle://owner:passwd@instance'

        logger = logging.getLogger()

        self.dbs2sqlapi = DBS2SqlApi(logger,connectUrlDBS2,ownerDBS2)
        self.dbs3sqlapi = DBS3SqlApi(logger,connectUrlDBS3,ownerDBS3,ownerDBS2)

    def test_acquisition_eras(self):
        resultsDBS2 = self.dbs2sqlapi.acquisitionEras(sort=False)
        resultsDBS3 = self.dbs3sqlapi.acquisitionEras(sort=False)

        self.assertTrue(listComparision(resultsDBS2,resultsDBS3))

    def test_application_executables(self):
        resultsDBS2 = self.dbs2sqlapi.applicationExecutables(sort=False)
        resultsDBS3 = self.dbs3sqlapi.applicationExecutables(sort=False)

        self.assertTrue(listComparision(resultsDBS2,resultsDBS3))

    def test_blocks(self):
        resultsDBS2 = self.dbs2sqlapi.blockList(sort=False)
        resultsDBS3 = self.dbs3sqlapi.blockList(sort=True)#not sorted in SQL Query

        self.assertTrue(listComparision(resultsDBS2,resultsDBS3))
        
    def test_block_parents(self):
        resultsUnion = self.dbs3sqlapi.blockParents(sort=False)
                    
        self.assertEqual(len(resultsUnion),0)
    
    def test_datasets(self):
        resultsDBS2 = self.dbs2sqlapi.datasetList(sort=False)
        resultsDBS3 = self.dbs3sqlapi.datasetList(sort=False)

        self.assertTrue(listComparision(resultsDBS2,resultsDBS3))

    def test_dataset_access_types(self):
        resultsDBS2 = self.dbs2sqlapi.datasetAccessTypes(sort=False)
        resultsDBS3 = self.dbs3sqlapi.datasetAccessTypes(sort=False)

        self.assertTrue(listComparision(resultsDBS2,resultsDBS3))

    def test_dataset_output_mod_configs(self):
        resultsDBS2 = self.dbs2sqlapi.datasetOutputModConfigs(sort=False)
        resultsDBS3 = self.dbs3sqlapi.datasetOutputModConfigs(sort=False)

        self.assertTrue(listComparision(resultsDBS2,resultsDBS3))

    def test_dataset_parents(self):
        resultsDBS2 = self.dbs2sqlapi.datasetParents(sort=False)
        resultsDBS3 = self.dbs3sqlapi.datasetParents(sort=False)
        
        self.assertTrue(listComparision(resultsDBS2,resultsDBS3))

    def test_dataset_runs(self):
        resultsDBS2 = self.dbs2sqlapi.datasetRuns(sort=False)
        resultsDBS3 = self.dbs3sqlapi.datasetRuns(sort=False)

        self.assertTrue(listComparision(resultsDBS2,resultsDBS3))
    
    def test_data_tiers(self):
        resultsDBS2 = self.dbs2sqlapi.dataTierList(sort=False)
        resultsDBS3 = self.dbs3sqlapi.dataTierList(sort=True)#not sorted in SQL Query

        self.assertTrue(listComparision(resultsDBS2,resultsDBS3))

    def test_files(self):
        resultsUnion = self.dbs3sqlapi.fileList(sort=False)
            
        self.assertEqual(len(resultsUnion),0)

    def test_file_data_types(self):
        resultsDBS2 = self.dbs2sqlapi.fileDataTypes(sort=False)
        resultsDBS3 = self.dbs3sqlapi.fileDataTypes(sort=False)

        diffKeys(resultsDBS2[0],resultsDBS3[0])

        self.assertTrue(listComparision(resultsDBS2,resultsDBS3))

    def test_file_lumis(self):
        resultsUnion = self.dbs3sqlapi.fileLumis(sort=False)
                    
        self.assertEqual(len(resultsUnion),0)
        
    def test_file_output_mod_configs(self):
        resultsUnion = self.dbs3sqlapi.fileOutputModConfigs(sort=False)
        
        self.assertEqual(len(resultsUnion),0)
    
    def test_file_parents(self):
        resultsUnion = self.dbs3sqlapi.fileParents(sort=False)
                    
        self.assertEqual(len(resultsUnion),0)
    
    def test_output_module_config(self):
        resultsDBS2 = self.dbs2sqlapi.outputModuleConfig(sort=False)
        resultsDBS3 = self.dbs3sqlapi.outputModuleConfig(sort=False)

        self.assertTrue(listComparision(resultsDBS2,resultsDBS3))

    def test_parameter_set_hashes(self):
        resultsDBS2 = self.dbs2sqlapi.parametersetHashes(sort=False)
        resultsDBS3 = self.dbs3sqlapi.parametersetHashes(sort=False)

        self.assertTrue(listComparision(resultsDBS2,resultsDBS3))

    def test_physics_groups(self):
        resultsDBS2 = self.dbs2sqlapi.physicsGroups(sort=False)
        resultsDBS3 = self.dbs3sqlapi.physicsGroups(sort=False)

        self.assertTrue(listComparision(resultsDBS2,resultsDBS3))

    def test_primary_datasets(self):
        resultsDBS2 = self.dbs2sqlapi.primaryDatasetList(sort=False)
        resultsDBS3 = self.dbs3sqlapi.primaryDatasetList(sort=True)#not sorted in SQL Query

        self.assertTrue(listComparision(resultsDBS2,resultsDBS3))

    def test_primary_ds_types(self):
        resultsDBS2 = self.dbs2sqlapi.primaryDSTypes(sort=False)
        resultsDBS3 = self.dbs3sqlapi.primaryDSTypes(sort=False)

        self.assertTrue(listComparision(resultsDBS2,resultsDBS3))
        
    def test_processed_datasets(self):
        resultsDBS2 = self.dbs2sqlapi.processedDatasets(sort=False)
        resultsDBS3 = self.dbs3sqlapi.processedDatasets(sort=False)

        self.assertTrue(listComparision(resultsDBS2,resultsDBS3))

    def test_release_versions(self):
        resultsDBS2 = self.dbs2sqlapi.releaseVersions(sort=False)
        resultsDBS3 = self.dbs3sqlapi.releaseVersions(sort=False)

        self.assertTrue(listComparision(resultsDBS2,resultsDBS3))

if __name__ == '__main__':
    TestSuite = unittest.TestSuite()
    TestSuite.addTest(CompareDBS2ToDBS3('test_acquisition_eras'))
    TestSuite.addTest(CompareDBS2ToDBS3('test_application_executables'))
    TestSuite.addTest(CompareDBS2ToDBS3('test_blocks'))
    TestSuite.addTest(CompareDBS2ToDBS3('test_block_parents'))
    TestSuite.addTest(CompareDBS2ToDBS3('test_data_tiers'))
    TestSuite.addTest(CompareDBS2ToDBS3('test_datasets'))
    TestSuite.addTest(CompareDBS2ToDBS3('test_dataset_access_types'))
    TestSuite.addTest(CompareDBS2ToDBS3('test_dataset_output_mod_configs'))
    TestSuite.addTest(CompareDBS2ToDBS3('test_dataset_parents'))
    TestSuite.addTest(CompareDBS2ToDBS3('test_files'))
    TestSuite.addTest(CompareDBS2ToDBS3('test_file_data_types'))
    TestSuite.addTest(CompareDBS2ToDBS3('test_file_lumis'))
    TestSuite.addTest(CompareDBS2ToDBS3('test_file_output_mod_configs'))
    TestSuite.addTest(CompareDBS2ToDBS3('test_file_parents'))
    TestSuite.addTest(CompareDBS2ToDBS3('test_output_module_config'))
    TestSuite.addTest(CompareDBS2ToDBS3('test_parameter_set_hashes'))
    TestSuite.addTest(CompareDBS2ToDBS3('test_physics_groups'))
    TestSuite.addTest(CompareDBS2ToDBS3('test_primary_datasets'))
    TestSuite.addTest(CompareDBS2ToDBS3('test_primary_ds_types'))
    TestSuite.addTest(CompareDBS2ToDBS3('test_processed_datasets'))
    TestSuite.addTest(CompareDBS2ToDBS3('test_release_versions'))
    
    unittest.TextTestRunner(verbosity=2).run(TestSuite)
    


