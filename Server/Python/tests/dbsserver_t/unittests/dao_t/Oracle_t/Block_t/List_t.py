"""
dao unittests
"""

__revision__ = "$Id: List_t.py,v 1.1 2010/01/01 19:54:40 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"

import os
import unittest
import logging
from WMCore.Database.DBFactory import DBFactory
from dbs.dao.Oracle.Block.List import List as BlockList

class List_t(unittest.TestCase):
    
    def setUp(self):
        """setup all necessary parameters"""
        dburl = os.environ["DBS_TEST_DBURL_READER"] 
        self.logger = logging.getLogger("dbs test logger")
        self.dbowner = os.environ["DBS_TEST_DBOWNER_READER"]
        self.dbi = DBFactory(self.logger, dburl).connect()
        self.assertTrue(self.dbi.engine.dialect.name == "oracle", \
                        "Database must be oracle" )
                        
    def test01(self):
        """dao.Oracle.Block.List: Basic"""
        dao = BlockList(self.logger, self.dbi, self.dbowner)
        dao.execute(dataset="*")
        dao.execute(block_name='*')
        dao.execute(site_name='*')
        result = dao.execute(block_name='*')
        self.assertTrue(type(result) == list)
        self.assertEqual(len(result), 0)
        
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(List_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
        
