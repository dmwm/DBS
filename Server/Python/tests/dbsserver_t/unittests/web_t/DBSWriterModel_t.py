"""
web unittests
"""

__revision__ = "$Id: DBSWriterModel_t.py,v 1.1 2010/01/01 19:54:38 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"

import os
import unittest
from dbsserver_t.utils.DBSRestApi import DBSRestApi

COUNTER = os.environ['DBS_TEST_COUNTER']

class DBSWriterModel_t(unittest.TestCase):
    
    def setUp(self):
        """setup all necessary parameters"""
        config = os.environ["DBS_TEST_CONFIG_WRITER"] 
        self.api = DBSRestApi(config) 

    def test01(self):
        """web.DBSReaderModel.insertPrimaryDataset: basic test"""
        data = {'primary_ds_name':'unittest_web_primary_ds_name_%s' % COUNTER,
                'primary_ds_type':'TEST'}
        self.api.insert('primarydatasets', data)
        
        
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSWriterModel_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
