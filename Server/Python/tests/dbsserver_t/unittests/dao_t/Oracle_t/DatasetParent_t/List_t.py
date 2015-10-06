"""
dao unittests
"""

__revision__ = "$Id: List_t.py,v 1.2 2010/03/23 16:25:06 akhukhun Exp $"
__version__ = "$Revision: 1.2 $"

import os
import unittest

from dbsserver_t.utils.DaoConfig import DaoConfig
from dbsserver_t.utils.DBSDataProvider import create_dbs_data_provider, strip_volatile_fields
from dbs.dao.Oracle.DatasetParent.List import List as DatasetParentList

class List_t(unittest.TestCase):
    @DaoConfig("DBSReader")
    def __init__(self, methodName='runTest'):
        super(List_t, self).__init__(methodName)
        data_location = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data.pkl')
        self.data_provider = create_dbs_data_provider(data_type='transient', data_location=data_location)
        self.data = self.data_provider.get_dataset_parentage_data()
       
    def setUp(self):
        """setup all necessary parameters"""
        self.conn = self.dbi.connection()
        self.dao = DatasetParentList(self.logger, self.dbi, self.dbowner)

    def tearDown(self):
        """Clean-up all necessary parameters"""
        self.conn.close()
                                                
    def test01(self):
        """dao.Oracle.DatasetParent.List: Basic"""
        result = self.dao.execute(self.conn, dataset=self.data[0]['this_dataset'])
        self.assertTrue(type(result) == list)
        self.assertEqual(strip_volatile_fields(result), self.data)

if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(List_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
        
