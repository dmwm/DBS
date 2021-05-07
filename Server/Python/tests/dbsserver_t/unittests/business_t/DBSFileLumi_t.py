"""
business unittests
"""

__revision__ = "$Id: DBSFileLumi_t.py,v 1.1 2010/01/01 19:54:37 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"

import os
import unittest
import logging
from WMCore.Database.DBFactory import DBFactory
from dbs.business.DBSFile import DBSFile

class DBSFileLumi_t(unittest.TestCase):
    """ Insert should go first - test01...
        This way it will be easy to do the validation test"""
    
    def setUp(self):
        """setup all necessary parameters"""
        self.logger = logging.getLogger("dbs test logger")

    def test02(self):
        """business.DBSFile.listFileLumis: Basic"""
        dburl = os.environ["DBS_TEST_DBURL_READER"] 
        dbowner = os.environ["DBS_TEST_DBOWNER_READER"]
        dbi = DBFactory(self.logger, dburl).connect()
        bo = DBSFile(self.logger, dbi, dbowner)

        bo.listFileLumis(logical_file_name='foo')
        result = bo.listFileLumis(block_name='foo')
        self.assertTrue(isinstance(result, list))
        self.assertEqual(len(result), 0)
        
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSFileLumi_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
