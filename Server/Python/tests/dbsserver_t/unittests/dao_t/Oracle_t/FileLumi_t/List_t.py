"""
dao unittests
"""

__revision__ = "$Id: List_t.py,v 1.2 2010/03/23 16:25:35 akhukhun Exp $"
__version__ = "$Revision: 1.2 $"

import os
import unittest
import logging

from dbsserver_t.utils.DaoConfig import DaoConfig
from dbsserver_t.utils.DBSDataProvider import create_dbs_data_provider, strip_volatile_fields
from dbs.dao.Oracle.FileLumi.List import List as FileLumiList

class List_t(unittest.TestCase):
    @DaoConfig("DBSReader")
    def __init__(self, methodName='runTest'):
        super(List_t,self).__init__(methodName)
        data_location = os.path.join(os.path.dirname(os.path.abspath(__file__)),'test_data.pkl')
        self.data_provider = create_dbs_data_provider(data_type='transient',data_location=data_location)
        self.lumi_data = self.data_provider.get_file_lumi_data()
        self.file_data = self.data_provider.get_file_data()
        #needs to be regenerated, since it was not used in Insert_t
        self.block_data = self.data_provider.get_block_data(regenerate=True) 
        
    def setUp(self):
        """setup all necessary parameters"""
        self.conn = self.dbi.connection()
        self.dao = FileLumiList(self.logger, self.dbi, self.dbowner)

    def tearDown(self):
        """Clean-up all necessary parameters"""
        self.conn.close()
                           
    def test01(self):
        """dao.Oracle.FileLumi.List: Basic"""
        result = self.dao.execute(self.conn, logical_file_name=self.file_data[0]['logical_file_name'])
        self.assertTrue(type(result) == list)
        self.assertEqual(strip_volatile_fields(result), self.lumi_data)

    def test02(self):
        """dao.Oracle.FileLumi.List: Basic"""
        result = self.dao.execute(self.conn, block_name=self.block_data[0]['block_name'])
        self.assertTrue(type(result) == list)
        self.assertEqual(strip_volatile_fields(result), self.lumi_data)
        
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(List_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
        
