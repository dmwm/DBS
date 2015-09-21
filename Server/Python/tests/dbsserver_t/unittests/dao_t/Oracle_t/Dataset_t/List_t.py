"""
dao unittests
"""

__revision__ = "$Id: List_t.py,v 1.2 2010/03/23 16:23:16 akhukhun Exp $"
__version__ = "$Revision: 1.2 $"

import os
import unittest

from dbsserver_t.utils.DaoConfig import DaoConfig
from dbsserver_t.utils.DBSDataProvider import create_dbs_data_provider, strip_volatile_fields
from dbs.dao.Oracle.Dataset.List import List as DatasetList
from types import  GeneratorType

class List_t(unittest.TestCase):
    @DaoConfig("DBSReader")
    def __init__(self, methodName='runTest'):
        super(List_t,self).__init__(methodName)
        data_location = os.path.join(os.path.dirname(os.path.abspath(__file__)),'test_data.pkl')
        self.data_provider = create_dbs_data_provider(data_type='transient',data_location=data_location)
        self.data = self.data_provider.get_dataset_data()
                
    def setUp(self):
        """setup all necessary parameters"""
        self.conn = self.dbi.connection()
        self.dao = DatasetList(self.logger, self.dbi, self.dbowner)

    def tearDown(self):
        """Clean-up all necessary parameters"""
        self.conn.close()
                        
    def test01(self):
        """dao.Oracle.Dataset.List: Basic"""
        result = self.dao.execute(self.conn)
        self.assertTrue(type(result) == GeneratorType)
                
    def test02(self):
        """dao.Oracle.Dataset.List: Basic"""
        r = self.dao.execute(self.conn, dataset=self.data[0]['dataset'])
	result = []
        for item in r:
	    result.append(item)	
        self.assertEqual(strip_volatile_fields(result), self.data)
        
    def test03(self):
        """dao.Oracle.Dataset.List: Basic"""
        result = self.dao.execute(self.conn, "ThisDoesNotExist")
        self.assertTrue(type(result) == GeneratorType)
	l = []
        for item in result:
            l = append(item)  	
        self.assertEqual(len(l), 0)

if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(List_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
        
