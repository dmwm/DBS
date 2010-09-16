"""
business unittests
"""

__revision__ = "$Id: DBSBlock_t.py,v 1.1 2010/01/01 19:54:37 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"

import os
import unittest
import logging
from WMCore.Database.DBFactory import DBFactory
from dbs.business.DBSBlock import DBSBlock

class DBSBlock_t(unittest.TestCase):
    
    def setUp(self):
        """setup all necessary parameters"""
        self.logger = logging.getLogger("dbs test logger")

    def test01(self):
        """business.DBSBlock.listBlocks: Basic"""
        dburl = os.environ["DBS_TEST_DBURL_READER"] 
        dbowner = os.environ["DBS_TEST_DBOWNER_READER"]
        dbi = DBFactory(self.logger, dburl).connect()
        bo = DBSBlock(self.logger, dbi, dbowner)

        result = bo.listBlocks(dataset='%')
        self.assertTrue(type(result) == list)
        self.assertEqual(len(result), 0)
        
        result = bo.listBlocks(block_name='%')
        self.assertTrue(type(result) == list)
        self.assertEqual(len(result), 0)
        
        result = bo.listBlocks(dataset = '%', site_name='%')
        self.assertTrue(type(result) == list)
        
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSBlock_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
        
