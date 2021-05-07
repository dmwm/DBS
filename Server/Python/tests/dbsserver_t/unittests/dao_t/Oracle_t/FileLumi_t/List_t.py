"""
dao unittests
"""
import os
import unittest
import logging
import copy

from dbsserver_t.utils.DaoConfig import DaoConfig
from dbsserver_t.utils.DBSDataProvider import create_dbs_data_provider, strip_volatile_fields
from dbs.dao.Oracle.FileLumi.List import List as FileLumiList
from types import GeneratorType

class List_t(unittest.TestCase):
    @DaoConfig("DBSReader")
    def __init__(self, methodName='runTest'):
        super(List_t, self).__init__(methodName)
        data_location = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data.pkl')
        self.data_provider = create_dbs_data_provider(data_type='transient', data_location=data_location)
        self.lumi_data = self.data_provider.get_file_lumi_data()
        #needs to be regenerated, since it was not used in Insert_t
        self.block_data = self.data_provider.get_block_data(regenerate=True) 
        
    def setUp(self):
        """setup all necessary parameters"""
        self.conn = self.dbi.connection()
        self.dao = FileLumiList(self.logger, self.dbi, self.dbowner)

        #List API returns a list of lumi sections, whereas the Insert API needs a single lumi_section_number per file
        #IMHO that should be fixed
        for entry in self.lumi_data:
            if 'lumi_section_num' in entry:
                entry['lumi_section_num'] = [entry['lumi_section_num']]

    def tearDown(self):
        """Clean-up all necessary parameters"""
        self.conn.close()
                           
    def test01(self):
        """dao.Oracle.FileLumi.List: Basic"""
        result = self.dao.execute(self.conn, logical_file_name=self.lumi_data[0]['logical_file_name'])

        self.assertTrue(isinstance(result, GeneratorType))
	l = []
	for i in result:
	    l.append(i)
        self.assertEqual(strip_volatile_fields(l), self.lumi_data)

    def test02(self):
        """dao.Oracle.FileLumi.List: Basic"""
        result = self.dao.execute(self.conn, block_name=self.block_data[0]['block_name'])
        self.assertTrue(isinstance(result, GeneratorType))
	l =[]
	for i in result:
	    l.append(i)
        self.assertEqual(strip_volatile_fields(l), self.lumi_data)
        
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(List_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
        
