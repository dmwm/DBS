"""
OutputModuleConfig.List unittests
"""

__revision__ = "$Id: List_t.py,v 1.3 2010/03/23 16:26:07 akhukhun Exp $"
__version__ = "$Revision: 1.3 $"

import os
import unittest

from dbsserver_t.utils.DaoConfig import DaoConfig
from dbsserver_t.utils.DBSDataProvider import create_dbs_data_provider, strip_volatile_fields
from dbs.dao.Oracle.OutputModuleConfig.List import List as OutputModuleConfigList

class List_t(unittest.TestCase):
    @DaoConfig("DBSReader")
    def __init__(self, methodName='runTest'):
        super(List_t,self).__init__(methodName)
        data_location = os.path.join(os.path.dirname(os.path.abspath(__file__)),'test_data.pkl')
        self.data_provider = create_dbs_data_provider(data_type='transient',data_location=data_location)
        self.data = self.data_provider.get_output_module_config_data()
        #remove create_by and creation_date from data, since DBS3 does not return it
        del self.data[0]['create_by']
        del self.data[0]['creation_date']
        
    def setUp(self):
        """setup all necessary parameters"""
        self.conn = self.dbi.connection()
        self.dao = OutputModuleConfigList(self.logger, self.dbi, self.dbowner)

    def tearDown(self):
        """Clean-up all necessary parameters"""
        self.conn.close()
                        
    def test01(self):
        """dao.Oracle.OutputModuleConfig.List: Basic"""
        result = self.dao.execute(self.conn)
        self.assertTrue(isinstance(result,list))
                        
    def test02(self):
        """dao.Oracle.OutputModuleConfig.List: Basic"""
        result = self.dao.execute(self.conn, release_version = self.data[0]['release_version'])
        self.data[0]['name'] = self.data[0].get('name', result[0]['name'])
        self.data[0]['create_by'] = self.data[0].get('create_by', result[0]['create_by'])
        self.data[0]['creation_date'] = self.data[0].get('creation_date', result[0]['creation_date'])
        self.assertEqual(strip_volatile_fields(result), self.data)

    def test05(self):
        """dao.Oracle.OutputModuleConfig.List: Basic"""
        result = self.dao.execute(self.conn, pset_hash = self.data[0]['pset_hash'])
        self.data[0]['name'] = self.data[0].get('name', result[0]['name'])
        self.data[0]['create_by'] = self.data[0].get('create_by', result[0]['create_by'])
        self.data[0]['creation_date'] = self.data[0].get('creation_date', result[0]['creation_date'])
        self.assertEqual(strip_volatile_fields(result), self.data)
        
    def test06(self):
        """dao.Oracle.OutputModuleConfig.List: Basic"""
        result = self.dao.execute(self.conn, app = self.data[0]['app_name'])
        self.data[0]['name'] = self.data[0].get('name', result[0]['name'])
        self.data[0]['create_by'] = self.data[0].get('create_by', result[0]['create_by'])
        self.data[0]['creation_date'] = self.data[0].get('creation_date', result[0]['creation_date'])
        self.assertTrue(self.data[0] in strip_volatile_fields(result))
        
    def test07(self):
        """dao.Oracle.OutputModuleConfig.List: Basic"""
        result = self.dao.execute(self.conn, output_label = self.data[0]['output_module_label'])
        self.data[0]['name'] = self.data[0].get('name', result[0]['name'])
        self.data[0]['create_by'] = self.data[0].get('create_by', result[0]['create_by'])
        self.data[0]['creation_date'] = self.data[0].get('creation_date', result[0]['creation_date'])
        self.assertEqual(strip_volatile_fields(result), self.data)
        
    def test08(self):
        """dao.Oracle.OutputModuleConfig.List: Basic"""
        result = self.dao.execute(self.conn, "ThisDoesNotExist")
        self.assertTrue(isinstance(result, list))
        self.assertEqual(len(result), 0)

if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(List_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
        
