"""
dao unittests
"""
import os
import unittest

from types import GeneratorType
from dbsserver_t.utils.DaoConfig import DaoConfig
from dbsserver_t.utils.DBSDataProvider import create_dbs_data_provider, strip_volatile_fields
from dbs.dao.Oracle.FileParent.List import List as FileParentList

class List_t(unittest.TestCase):
    @DaoConfig("DBSReader")
    def __init__(self, methodName='runTest'):
        super(List_t, self).__init__(methodName)
        data_location = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data.pkl')
        self.data_provider = create_dbs_data_provider(data_type='transient', data_location=data_location)
        self.data = self.data_provider.get_file_parentage_data()

    def setUp(self):
        """setup all necessary parameters"""
        self.conn = self.dbi.connection()
        self.dao = FileParentList(self.logger, self.dbi, self.dbowner)

    def tearDown(self):
        """Clean-up all necessary parameters"""
        self.conn.close()
                    
    def test01(self):
        """dao.Oracle.FileParent.List: Basic"""
        result = self.dao.execute(self.conn, logical_file_name=self.data[0]['this_logical_file_name'])
        self.assertTrue(isinstance(result, GeneratorType))
	l = []
	for item in result:
	    l.append(item)
        self.assertEqual(strip_volatile_fields(l), self.data)
        
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(List_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
        
