"""
dao unittests
"""

__revision__ = "$Id: List_t.py,v 1.2 2010/03/23 16:25:35 akhukhun Exp $"
__version__ = "$Revision: 1.2 $"

import os
import unittest
import logging
from WMCore.Database.DBFactory import DBFactory
from dbs.dao.Oracle.FileLumi.List import List as FileLumiList

class List_t(unittest.TestCase):
    
    def setUp(self):
        """setup all necessary parameters"""
        dburl = os.environ["DBS_TEST_DBURL_READER"] 
        self.logger = logging.getLogger("dbs test logger")
        self.dbowner = os.environ["DBS_TEST_DBOWNER_READER"]
        self.dbi = DBFactory(self.logger, dburl).connect()
                        
    def test01(self):
        """dao.Oracle.FileLumi.List: Basic"""
	conn = self.dbi.connection()
        dao = FileLumiList(self.logger, self.dbi, self.dbowner)
        result = dao.execute(conn, logical_file_name='%')
        self.assertTrue(type(result) == list)
        self.assertEqual(len(result), 0)
        result = dao.execute(conn, block_name='%')
        self.assertTrue(type(result) == list)
        self.assertEqual(len(result), 0)
	conn.close()
        
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(List_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
        
