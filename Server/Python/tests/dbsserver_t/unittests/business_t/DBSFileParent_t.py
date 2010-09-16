"""
business unittests
"""

__revision__ = "$Id: DBSFileParent_t.py,v 1.1 2010/01/01 19:54:37 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"

import os
import unittest
import logging
from WMCore.Database.DBFactory import DBFactory
from dbs.business.DBSFileParent import DBSFileParent

class DBSFileParent_t(unittest.TestCase):
    """ Insert should go first - test01...
        This way it will be easy to do the validation test"""
    
    def setUp(self):
        """setup all necessary parameters"""
        self.logger = logging.getLogger("dbs test logger")

    def test02(self):
        """business.DBSFile.listFileParents: Basic"""
        dburl = os.environ["DBS_TEST_DBURL_READER"] 
        dbowner = os.environ["DBS_TEST_DBOWNER_READER"]
        dbi = DBFactory(self.logger, dburl).connect()
        bo = DBSFileParent(self.logger, dbi, dbowner)

        result = bo.listFileParents(logical_file_name='%')
        self.assertTrue(type(result) == list)
        self.assertEqual(len(result), 0)
        
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSFileParent_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
