"""
business unittests
"""

__revision__ = "$Id: DBSPrimaryDataset_t.py,v 1.1 2010/01/01 19:54:37 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"

import os
import unittest
import logging
from WMCore.Database.DBFactory import DBFactory
from dbs.business.DBSPrimaryDataset import DBSPrimaryDataset

COUNTER = os.environ['DBS_TEST_COUNTER']

class DBSPrimaryDataset_t(unittest.TestCase):
    
    def setUp(self):
        """setup all necessary parameters"""
        self.logger = logging.getLogger("dbs test logger")
        self.primary_ds_name =  'unittest_business_primary_ds_name_%s' % COUNTER
        
    def test01(self):
        """business.DBSPrimaryDataset.insertPrimaryDatasets: Basic"""
        dburl = os.environ["DBS_TEST_DBURL_WRITER"] 
        dbowner = os.environ["DBS_TEST_DBOWNER_WRITER"]
        dbi = DBFactory(self.logger, dburl).connect()
        
        bo = DBSPrimaryDataset(self.logger, dbi, dbowner)
        binput = {'primary_ds_name':self.primary_ds_name,
		          'primary_ds_type':'TEST'}
        bo.insertPrimaryDataset(binput)

    def test02(self):
        """business.DBSPrimaryDataset.listPrimaryDatasets: Basic"""
        dbowner = os.environ["DBS_TEST_DBOWNER_READER"]
        dburl = os.environ["DBS_TEST_DBURL_READER"] 
        dbi = DBFactory(self.logger, dburl).connect()
        bo = DBSPrimaryDataset(self.logger, dbi, dbowner)

        bo.listPrimaryDatasets()
        bo.listPrimaryDatasets(primary_ds_name='%')
        result = bo.listPrimaryDatasets("ThisDoesNotExist")
        self.assertTrue(type(result) == list)
        self.assertEqual(len(result), 0)

    def test03(self):
        """business.DBSPrimaryDataset.listPrimaryDatasets: Validation"""
        dbowner = os.environ["DBS_TEST_DBOWNER_READER"]
        dburl = os.environ["DBS_TEST_DBURL_READER"] 
        dbi = DBFactory(self.logger, dburl).connect()
        bo = DBSPrimaryDataset(self.logger, dbi, dbowner)

        result = bo.listPrimaryDatasets(primary_ds_name=self.primary_ds_name)
        self.assertTrue(type(result) == list)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["primary_ds_name"], self.primary_ds_name)
        
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSPrimaryDataset_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
