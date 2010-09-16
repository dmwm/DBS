"""
business unittests
"""

__revision__ = "$Id: DBSFile_t.py,v 1.1 2010/01/01 19:54:37 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"

import os
import unittest
import logging
from WMCore.Database.DBFactory import DBFactory
from dbs.business.DBSFile import DBSFile

class DBSFile_t(unittest.TestCase):
    
    def setUp(self):
        """setup all necessary parameters"""
        self.logger = logging.getLogger("dbs test logger")

    def testDBSFileList(self):
        """business.DBSFile.listFiles: Basic"""
        dburl = os.environ["DBS_TEST_DBURL_READER"] 
        dbowner = os.environ["DBS_TEST_DBOWNER_READER"]
        dbi = DBFactory(self.logger, dburl).connect()
        bo = DBSFile(self.logger, dbi, dbowner)

        result = bo.listFiles('%')
        self.assertTrue(type(result) == list)
        self.assertEqual(len(result), 0)
        
        result = bo.listFiles(dataset='%')
        self.assertTrue(type(result) == list)
        self.assertEqual(len(result), 0)
        
        result = bo.listFiles(block_name='%')
        self.assertTrue(type(result) == list)
        self.assertEqual(len(result), 0)
        
        result = bo.listFiles(logical_file_name='%')
        self.assertTrue(type(result) == list)
        self.assertEqual(len(result), 0)
        
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSFile_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
        
