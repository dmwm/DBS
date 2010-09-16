"""
web unittests
"""

__revision__ = "$Id: DBSReaderModel_t.py,v 1.3 2010/01/13 22:34:21 afaq Exp $"
__version__ = "$Revision: 1.3 $"

import os
import json
import unittest
from dbsserver_t.utils.DBSRestApi import DBSRestApi

class DBSReaderModel_t(unittest.TestCase):
    
    	
    def setUp(self):
        """setup all necessary parameters"""
        config = os.environ["DBS_TEST_CONFIG_READER"] 
        self.api = DBSRestApi(config) 

    def test01(self):
        """web.DBSReaderModel.listPrimaryDatasets: basic test"""
        self.api.list('primarydatasets')
        self.api.list('primarydatasets', primary_ds_name='*')
        
    def test02(self):
        """web.DBSReaderModel.listDatasets: basic test"""
        self.api.list('datasets')
        self.api.list('datasets', dataset='*')
        self.api.list('datasets', parent_dataset='*')
        self.api.list('datasets', release_version='*')
        self.api.list('datasets', pset_hash='*')
        self.api.list('datasets', app_name='*')
        self.api.list('datasets', output_module_label='*')
        self.api.list('datasets', dataset='*', 
                                  parent_dataset='*',
                                  release_version='*',
                                  pset_hash='*',
                                  app_name='*',
                                  output_module_label='*')
    
    def test03(self):
        """web.DBSReaderModel.listBlocks: basic test"""
	try:
	    self.api.list('blocks', dataset='*')
	    self.api.list('blocks', block_name='*')
	    self.api.list('blocks', site_name='*')
	    self.api.list('blocks', dataset='*', 
                                block_name='*',
                                site_name='*')
        except:
	    pass
	else:
	    self.fail("Exception was expected and was not raised.")
	    
    def test04(self):
        """web.DBSReaderModel.listBlocks: takes exact dataset name, not pattern"""
	try:
	    result=self.api.list('blocks', dataset='*')
	    import pdb
	    pdb.set_trace()
	except:
	    pass
	else:
	    self.fail("Exception was expected and was not raised.")
        
    def test05(self):
        """web.DBSReaderModel.listBlocks: Must raise an exception if no parameter is passed."""
	
        try:
	    self.api.list('blocks')
        except: 
	    pass
        else: 
	    self.fail("Exception was expected and was not raised.")
            
    def test06(self):
        """web.DBSReaderModel.listFiles: basic test"""
        self.api.list('files', dataset='*')
        self.api.list('files', block_name='*')
        self.api.list('files', logical_file_name='*')
        
    def test07(self):
        """web.DBSReaderModel.listFiles: Takes exact dataset, block_name or logical_file_name, not pattern."""
        result = self.api.list('files', dataset='*')
        result = json.loads(result)
        self.assertTrue(type(result) == list)
        self.assertEqual(len(result),0)
        result = self.api.list('files', block_name='*')
        result = json.loads(result)
        self.assertTrue(type(result) == list)
        self.assertEqual(len(result),0)
        result = self.api.list('files', logical_file_name='*')
        result = json.loads(result)
        self.assertTrue(type(result) == list)
        self.assertEqual(len(result),0)
        
    def test08(self):
        """web.DBSReaderModel.listFiles: Must raise an exception if no parameter is passed."""
        try: self.api.list('files')
        except: pass
        else: self.fail("Exception was expected and was not raised")
            
    def test09(self):
        """web.DBSReaderModel.listDatasetParents: basic test"""
        self.api.list('datasetparents', dataset="*")
        
    def test10(self):
        """web.DBSReaderModel.listDatasetParents: must raise an exception if no parameter is passed"""
        try: self.api.list('datasetparents')
        except: pass
        else: self.fail("Exception was expected and was not raised")
            
    def test11(self):
        """web.DBSReaderModel.listOutputConfigs: basic test"""
        self.api.list('outputconfigurations')
        self.api.list('outputconfigurations', dataset="*")
        self.api.list('outputconfigurations', logical_file_name="*")
        self.api.list('outputconfigurations', release_version="*")
        self.api.list('outputconfigurations', pset_hash="*")
        self.api.list('outputconfigurations', app_name="*")
        self.api.list('outputconfigurations', output_module_label="*")
        self.api.list('outputconfigurations', dataset="*",
                                              logical_file_name="*",
                                              release_version="*",
                                              pset_hash="*",
                                              app_name="*",
                                              output_module_label="*")
    
    def test12(self):
        """web.DBSReaderModel.listFileParents: basic test"""
        self.api.list('fileparents', logical_file_name="*")
    
    def test13(self):
        """web.DBSReaderModel.listFileParents: must raise an exception if no parameter is passed"""
        try: self.api.list('fileparents')
        except: pass
        else: self.fail("Exception was expected and was not raised")
        
    def test14(self):
        """web.DBSReaderModel.listFileLumis: basic test"""
        self.api.list('filelumis', logical_file_name="*")
        self.api.list('filelumis', block_name="*")
        
    def test15(self):
        """web.DBSReaderModel.listFileLumis: must raise an exception if no parameter is passed"""
        try: self.api.list('filelumis')
        except: pass
        else: self.fail("Exception was expected and was not raised")
        
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSReaderModel_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
        
