import itertools
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

class CompareDBS2ToDBS3(unittest.TestCase):
    
    def setUp(self):
        ownerDBS3 = 'owner'
        connectUrlDBS3 = 'oracle://owner:passwd@instance'

        ownerDBS2 = 'owner'
        connectUrlDBS2 = 'oracle://owner:passwd@instance'

        logger = logging.getLogger()

        self.dbs2sqlapi = DBS2SqlApi(logger,connectUrlDBS2,ownerDBS2)
        self.dbs3sqlapi = DBS3SqlApi(logger,connectUrlDBS3,ownerDBS3)
        
    def test_blocks(self):
        resultsDBS2 = self.dbs2sqlapi.blockList(sort=True)
        resultsDBS3 = self.dbs3sqlapi.blockList(sort=True)
                    
        self.assertTrue(listComparision(resultsDBS2,resultsDBS3))
     
    def test_datasets(self):
        resultsDBS2 = self.dbs2sqlapi.datasetList(sort=True)
        resultsDBS3 = self.dbs3sqlapi.datasetList(sort=True)

        self.assertTrue(listComparision(resultsDBS2,resultsDBS3))
        
    def test_data_tiers(self):
        resultsDBS2 = self.dbs2sqlapi.dataTierList(sort=True)
        resultsDBS3 = self.dbs3sqlapi.dataTierList(sort=True)

        self.assertTrue(listComparision(resultsDBS2,resultsDBS3))

    def test_files(self):
        minmaxcountDBS2 = self.dbs2sqlapi.fileMinMaxCount(sort=False)[0]
        minmaxcountDBS3 = self.dbs3sqlapi.fileMinMaxCount(sort=False)[0]

        stepWidth = 100000

        results = []

        step = 0

        estimatedSteps = ((minmaxcountDBS3['maximum']-minmaxcountDBS2['minimum'])/stepWidth)+1
        
        while True:

            minIDDBS2 = minmaxcountDBS2['minimum']+step*stepWidth
            maxIDDBS2 = min(minIDDBS2+stepWidth,minmaxcountDBS2['maximum'])

            minIDDBS3 = minmaxcountDBS3['minimum']+step*stepWidth
            maxIDDBS3 = min(minIDDBS3+stepWidth,minmaxcountDBS3['maximum'])

            resultsDBS2 = self.dbs2sqlapi.fileList(minimum=minIDDBS2,maximum=maxIDDBS2,sort=True)
            resultsDBS3 = self.dbs3sqlapi.fileList(minimum=minIDDBS3,maximum=maxIDDBS3,sort=True)

            print len(resultsDBS2),len(resultsDBS3)

            results.append(listComparision(resultsDBS2,resultsDBS3))

            print "%s/%s completed" % (step,estimatedSteps)

            step += 1

            if minIDDBS2 > minmaxcountDBS2['maximum'] or minIDDBS3 > minmaxcountDBS3['maximum']:
                break
            
        self.assertTrue(0 not in results)
        


    def test_primary_datasets(self):
        resultsDBS2 = self.dbs2sqlapi.primaryDatasetList(sort=True)
        resultsDBS3 = self.dbs3sqlapi.primaryDatasetList(sort=True)

        self.assertTrue(listComparision2(resultsDBS2,resultsDBS3))

if __name__ == '__main__':
    TestSuite = unittest.TestSuite()
    TestSuite.addTest(CompareDBS2ToDBS3('test_blocks'))
    TestSuite.addTest(CompareDBS2ToDBS3('test_data_tiers'))
    TestSuite.addTest(CompareDBS2ToDBS3('test_datasets'))
    TestSuite.addTest(CompareDBS2ToDBS3('test_files'))
    TestSuite.addTest(CompareDBS2ToDBS3('test_primary_datasets'))
    unittest.TextTestRunner(verbosity=2).run(TestSuite)
    


