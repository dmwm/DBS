"""
Unittests to validate the DBS2 to DBS3 migration
"""

import logging
import unittest

from DBSSqlQueries import DBSSqlQueries

try:
    from DBSSecret import DBS2Secret
    from DBSSecret import DBS3Secret
except:
    msg = """You need to put a DBSSecret.py in your directory. It has to have the following structure:\n
              DBS2Secret = {'connectUrl' : {
                            'reader' : 'oracle://reader:passwd@instance'
                            },
                            'databaseOwner' : 'owner'}
              DBS3Secret = {'connectUrl' : {
                            'reader' : 'oracle://reader:passwd@instance'
                            },
                            'databaseOwner' : 'owner'}"""
    print msg

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
        ownerDBS3 = DBS3Secret['databaseOwner']
        connectUrlDBS3 = DBS3Secret['connectUrl']['reader']

        ownerDBS2 = DBS2Secret['databaseOwner']
        connectUrlDBS2 = DBS2Secret['connectUrl']['reader']

        logger = logging.getLogger()

        self.dbssqlqueries = DBSSqlQueries(logger,connectUrlDBS3,ownerDBS3,ownerDBS2)

    def test_acquisition_eras(self):
        resultsUnion = self.dbssqlqueries.acquisitionEras(sort=False)
        
        self.assertEqual(len(resultsUnion),0)
        
    def test_application_executables(self):
        resultsUnion = self.dbssqlqueries.applicationExecutables(sort=False)

        self.assertEqual(len(resultsUnion),0)

    def test_blocks(self):
        resultsUnion = self.dbssqlqueries.block(sort=False)

        self.assertEqual(len(resultsUnion),0)
        
    def test_block_parents(self):
        resultsUnion = self.dbssqlqueries.blockParents(sort=False)
                    
        self.assertEqual(len(resultsUnion),0)
    
    def test_datasets(self):
        resultsUnion = self.dbssqlqueries.dataset(sort=False)
        
        self.assertEqual(len(resultsUnion),0)

    def test_dataset_access_types(self):
        resultsUnion = self.dbssqlqueries.datasetAccessTypes(sort=False)

        self.assertEqual(len(resultsUnion),0)

    def test_dataset_output_mod_configs(self):
        resultsUnion = self.dbssqlqueries.datasetOutputModConfigs(sort=False)

        self.assertEqual(len(resultsUnion),0)

    def test_dataset_parents(self):
        resultsUnion = self.dbssqlqueries.datasetParents(sort=False)

        self.assertEqual(len(resultsUnion),0)
    
    def test_data_tiers(self):
        resultsUnion = self.dbssqlqueries.dataTier(sort=False)

        self.assertEqual(len(resultsUnion),0)

    def test_files(self):
        resultsUnion = self.dbssqlqueries.file(sort=False)
            
        self.assertEqual(len(resultsUnion),0)

    def test_file_data_types(self):
        resultsUnion = self.dbssqlqueries.fileDataTypes(sort=False)

        self.assertEqual(len(resultsUnion),0)

    def test_file_lumis(self):
        resultsUnion = self.dbssqlqueries.fileLumis(sort=False)
                    
        self.assertEqual(len(resultsUnion),0)
        
    def test_file_output_mod_configs(self):
        resultsUnion = self.dbssqlqueries.fileOutputModConfigs(sort=False)
        
        self.assertEqual(len(resultsUnion),0)
    
    def test_file_parents(self):
        resultsUnion = self.dbssqlqueries.fileParents(sort=False)
                    
        self.assertEqual(len(resultsUnion),0)
    
    def test_output_module_config(self):
        resultsUnion = self.dbssqlqueries.outputModuleConfig(sort=False)

        self.assertEqual(len(resultsUnion),0)

    def test_parameter_set_hashes(self):
        resultsUnion = self.dbssqlqueries.parametersetHashes(sort=False)

        self.assertEqual(len(resultsUnion),0)

    def test_physics_groups(self):
        resultsUnion = self.dbssqlqueries.physicsGroups(sort=False)

        self.assertEqual(len(resultsUnion),0)

    def test_primary_datasets(self):
        resultsUnion = self.dbssqlqueries.primaryDataset(sort=False)

        self.assertEqual(len(resultsUnion),0)

    def test_primary_ds_types(self):
        resultsUnion = self.dbssqlqueries.primaryDSTypes(sort=False)

        self.assertEqual(len(resultsUnion),0)
        
    def test_processed_datasets(self):
        resultsUnion= self.dbssqlqueries.processedDatasets(sort=False)

        self.assertEqual(len(resultsUnion),0)

    def test_release_versions(self):
        resultsUnion = self.dbssqlqueries.releaseVersions(sort=False)
       
        self.assertEqual(len(resultsUnion),0)

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
    


