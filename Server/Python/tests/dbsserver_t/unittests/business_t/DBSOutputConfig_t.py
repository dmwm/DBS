"""
business unittests
"""

__revision__ = "$Id: DBSOutputConfig_t.py,v 1.1 2010/01/01 19:54:37 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"

import os
import unittest
import logging
from WMCore.Database.DBFactory import DBFactory
from dbs.business.DBSOutputConfig import DBSOutputConfig

class DBSOutputConfig_t(unittest.TestCase):
    """ Insert should go first - test01...
        This way it will be easy to do the validation test"""
    
    def setUp(self):
        """setup all necessary parameters"""
        self.logger = logging.getLogger("dbs test logger")

    def test02(self):
        """business.DBSOutputConfig.listOutputConfig: Basic"""
        dburl = os.environ["DBS_TEST_DBURL_READER"] 
        dbowner = os.environ["DBS_TEST_DBOWNER_READER"]
        dbi = DBFactory(self.logger, dburl).connect()
        bo = DBSOutputConfig(self.logger, dbi, dbowner)

        bo.listOutputConfigs()
        bo.listOutputConfigs(dataset='%')
        bo.listOutputConfigs(logical_file_name='%')
        bo.listOutputConfigs(version='%')
        bo.listOutputConfigs(hash='%')
        bo.listOutputConfigs(app_name='%')
        bo.listOutputConfigs(output_module_label='%')
        
        bo.listOutputConfigs(dataset='%', version='%')
        
        
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSOutputConfig_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
