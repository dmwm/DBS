"""
PrimaryDSType.List unittests
"""
import os
import unittest

from dbsserver_t.utils.DaoConfig import DaoConfig
from dbsserver_t.utils.DBSDataProvider import create_dbs_data_provider
from dbs.dao.Oracle.PrimaryDSType.List import List as PrimaryDSTypeList

class List_t(unittest.TestCase):
    @DaoConfig("DBSReader")
    def __init__(self, methodName='runTest'):
        super(List_t,self).__init__(methodName)
        #use persistent data, since inserts are not foreseen
        self.data_provider = create_dbs_data_provider(data_type='persistent')
        self.data = set((data['data_type'] for data in self.data_provider.get_primary_ds_type_data()))

    def setUp(self):
        """setup all necessary parameters"""
        self.conn = self.dbi.connection()
        self.dao = PrimaryDSTypeList(self.logger, self.dbi, self.dbowner)

    def tearDown(self):
        """Clean-up all necessary parameters"""
        self.conn.close()

    def test01(self):
        """dao.Oracle.PrimaryDSType.List: Basic"""
        result = self.dao.execute(self.conn)
        self.assertTrue(isinstance(result, list))

    def test02(self):
        """dao.Oracle.PrimaryDSType.List: Basic"""
        result = set((data['data_type'] for data in self.dao.execute(self.conn, dsType="%")))
        self.assertTrue(self.data.issubset(result))

    def test03(self):
        """dao.Oracle.PrimaryDSType.List: Basic"""
        result = self.dao.execute(self.conn, "ThisDoesNotExist")
        self.assertTrue(isinstance(result, list))
        self.assertEqual(len(result), 0)

if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(List_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
