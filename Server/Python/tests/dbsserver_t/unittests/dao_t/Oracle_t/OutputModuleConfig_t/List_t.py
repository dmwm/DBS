"""
dao unittests
"""

__revision__ = "$Id: List_t.py,v 1.3 2010/03/23 16:26:07 akhukhun Exp $"
__version__ = "$Revision: 1.3 $"

import os
import unittest
import logging
from WMCore.Database.DBFactory import DBFactory
from dbs.dao.Oracle.OutputModuleConfig.List import List as OutputModuleConfigList

class List_t(unittest.TestCase):
    
    def setUp(self):
        """setup all necessary parameters"""
        dburl = os.environ["DBS_TEST_DBURL_READER"] 
        self.logger = logging.getLogger("dbs test logger")
        self.dbowner = os.environ["DBS_TEST_DBOWNER_READER"]
        self.dbi = DBFactory(self.logger, dburl).connect()
                        
    def test01(self):
        """dao.Oracle.OutputModuleConfig.List: Basic"""
	conn = self.dbi.connection()
        dao = OutputModuleConfigList(self.logger, self.dbi, self.dbowner)
        dao.execute(conn)
        dao.execute(conn, dataset = '%')
        dao.execute(conn, logical_file_name = '%')
        dao.execute(conn, release_version = '%')
        dao.execute(conn, pset_hash = '%')
        dao.execute(conn, app = '%')
        dao.execute(conn, output_label = '%')
	conn.close()
        
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(List_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
        
