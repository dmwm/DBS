"""
PrimaryDataset.List unittests
"""

__revision__ = "$Id: List_t.py,v 1.2 2010/03/23 16:26:18 akhukhun Exp $"
__version__ = "$Revision: 1.2 $"

import os
import unittest

from dbsserver_t.utils.DaoConfig import DaoConfig
from dbsserver_t.utils.DBSDataProvider import create_dbs_data_provider, strip_volatile_fields
from dbs.dao.Oracle.PrimaryDataset.List import List as PrimaryDatasetList

class List_t(unittest.TestCase):
    @DaoConfig("DBSReader")
    def __init__(self, methodName='runTest'):
        super(List_t, self).__init__(methodName)
        data_location = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data.pkl')
        self.data_provider = create_dbs_data_provider(data_type='transient', data_location=data_location)
        self.data = self.data_provider.get_primary_dataset_data()
        
    def setUp(self):
        """setup all necessary parameters"""
        self.conn = self.dbi.connection()
        self.dao = PrimaryDatasetList(self.logger, self.dbi, self.dbowner)
                
    def tearDown(self):
        """Clean-up all necessary parameters"""
        self.conn.close()
    
    def test01(self):
        """dao.Oracle.PrimaryDataset.List: Basic"""
        result = self.dao.execute(self.conn)
        self.assertTrue(isinstance(result, list))
               
    def test02(self):
        """dao.Oracle.PrimaryDataset.List: Basic"""
        result = self.dao.execute(self.conn, primary_ds_name=self.data[0]['primary_ds_name'])
        self.assertEqual(strip_volatile_fields(result), self.data)
        
    def test03(self):
        """dao.Oracle.PrimaryDataset.List: Basic"""
        result = self.dao.execute(self.conn, "ThisDoesNotExist")
        self.assertTrue(isinstance(result, list))
        self.assertEqual(len(result), 0)
        
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(List_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
