"""
business unittests
"""

__revision__ = "$Id: DBSDataset_t.py,v 1.2 2010/01/07 17:38:52 afaq Exp $"
__version__ = "$Revision: 1.2 $"

import os
import unittest
import logging
from WMCore.Database.DBFactory import DBFactory
from dbs.business.DBSDataset import DBSDataset

class DBSDataset_t(unittest.TestCase):
    """ Insert should go first - test01...
        This way it will be easy to do the validation test"""
    
    def setUp(self):
        """setup all necessary parameters"""
        self.logger = logging.getLogger("dbs test logger")

    def test02(self):
        """business.DBSDataset.listDatasets: Basic"""
        dburl = os.environ["DBS_TEST_DBURL_READER"] 
        dbowner = os.environ["DBS_TEST_DBOWNER_READER"]
        dbi = DBFactory(self.logger, dburl).connect()
        bo = DBSDataset(self.logger, dbi, dbowner)

        bo.listDatasets()
        bo.listDatasets(dataset='%')
        result = bo.listDatasets("ThisDoesNotExist")
        self.assertTrue(type(result) == list)
        self.assertEqual(len(result), 0)
    
    def test03(self):
        """business.DBSDataset.listDatasets: with parent_dataset, release_version, pset_hash, app_name, output_module_label"""
        dburl = os.environ["DBS_TEST_DBURL_READER"] 
        dbowner = os.environ["DBS_TEST_DBOWNER_READER"]
        dbi = DBFactory(self.logger, dburl).connect()
        bo = DBSDataset(self.logger, dbi, dbowner)

        bo.listDatasets()
        bo.listDatasets(dataset='%')
        bo.listDatasets(dataset='%', release_version='%')
        bo.listDatasets(pset_hash='%')
        bo.listDatasets(app_name='%')
        bo.listDatasets(output_module_label='%')
        result = bo.listDatasets("ThisDoesNotExist")
        self.assertTrue(type(result) == list)
        self.assertEqual(len(result), 0)
        
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSDataset_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
