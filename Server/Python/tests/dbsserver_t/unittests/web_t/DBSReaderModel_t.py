"""
web unittests
"""

__revision__ = "$Id: DBSReaderModel_t.py,v 1.5 2010/01/25 18:07:10 yuyi Exp $"
__version__ = "$Revision: 1.5 $"

import os
import json
import unittest
from dbsserver_t.utils.DBSRestApi import DBSRestApi

class DBSReaderModel_t(unittest.TestCase):
    
    	
    def setUp(self):
        """setup all necessary parameters"""
	#import pdb
	#pdb.set_trace()
		
        config = os.environ["DBS_TEST_CONFIG_READER"] 
        self.api = DBSRestApi(config) 

    def test01(self):
        """Test01 web.DBSReaderModel.listPrimaryDatasets: basic test"""
        self.api.list('primarydatasets')
       
    def test02(self):
	"""Test02 web.DBSReaderModel.listPrimaryDatasets: basic test"""
	self.api.list('primarydatasets', primary_ds_name='*')
       
    def test03(self):
        """Test03 web.DBSReaderModel.listDatasets: basic test"""
        self.api.list('datasets')
    
    def test04(self):
        """Test04 web.DBSReaderModel.listDatasets: basic test"""
        self.api.list('datasets', dataset='*')

    def test05(self):
        """Test05 web.DBSReaderModel.listDatasets: basic test"""
        self.api.list('datasets', parent_dataset='*')
    
    def test06(self):
        """Test06 web.DBSReaderModel.listDatasets: basic test"""
        self.api.list('datasets', release_version='*')

    def test07(self):
        """Test07 web.DBSReaderModel.listDatasets: basic test"""
        self.api.list('datasets', pset_hash='*')

    def test08(self):
        """Test08 web.DBSReaderModel.listDatasets: basic test"""
        self.api.list('datasets', app_name='*')
    
    def test09(self):
        """Test09 web.DBSReaderModel.listDatasets: basic test"""
	self.api.list('datasets', output_module_label='*')

    def test10(self):
        """Test10 web.DBSReaderModel.listDatasets: basic test"""
	self.api.list('datasets', dataset='*', 
                                  parent_dataset='*',
                                  release_version='*',
                                  pset_hash='*',
                                  app_name='*',
                                  output_module_label='*')
    def test11(self):
        """Test11 web.DBSReaderModel.listBlocks: basic test"""
	try:
	    self.api.list('blocks', dataset='*')
        except:
	    pass
	else:
	    self.fail("Exception was expected and was not raised.")

    def test12(self):
        """Test12 web.DBSReaderModel.listBlocks: basic test"""
        try:
            self.api.list('blocks', block_name='*')
        except:
            pass
        else:
            self.fail("Exception was expected and was not raised.")

    def test13(self):
        """Test13 web.DBSReaderModel.listBlocks: basic test"""
        try:
            self.api.list('blocks', site_name='*')
        except:
            pass
        else:
            self.fail("Exception was expected and was not raised.")

    def test14(self):
        """Test14 web.DBSReaderModel.listBlocks: basic test"""
        try:
            self.api.list('blocks', dataset='*',
                                block_name='*',
                                site_name='*')
        except:
            pass
        else:
            self.fail("Exception was expected and was not raised.")


	    
    def test15(self):
        """Test15 web.DBSReaderModel.listBlocks: takes exact dataset name, not pattern"""
	try:
	    result=self.api.list('blocks', dataset='*')
	    #import pdb
	    #pdb.set_trace()
	except:
	    pass
	else:
	    self.fail("Exception was expected and was not raised.")
        
    def test16(self):
        """Test16 web.DBSReaderModel.listBlocks: Must raise an exception if no parameter is passed."""
	
        try:
	    self.api.list('blocks')
        except: 
	    pass
        else: 
	    self.fail("Exception was expected and was not raised.")
            
    def test17(self):
        """Test17 web.DBSReaderModel.listFiles: basic test"""
	try:
	    self.api.list('files', dataset='*')
	except:
	    pass
	else:
	    self.fail("Exception was expected and was not raised.")

    def test18(self):
        """Test18 web.DBSReaderModel.listFiles: basic test"""
	try:
	    self.api.list('files', block_name='*')
	except:
            pass
        else:
            self.fail("Exception was expected and was not raised.")


    def test19(self):
        """Test19 web.DBSReaderModel.listFiles: basic test"""
	try:
	    self.api.list('files', logical_file_name='*')
	except:
            pass
        else:
            self.fail("Exception was expected and was not raised.")
       
    def test21(self):
        """Test21 web.DBSReaderModel.listFiles: Must raise an exception if no parameter is passed."""
        try: self.api.list('files')
        except: pass
        else: self.fail("Exception was expected and was not raised")
            
    def test22(self):
        """Test22 web.DBSReaderModel.listDatasetParents: basic test"""
        self.api.list('datasetparents', dataset="*")
        
    def test23(self):
        """Test23 web.DBSReaderModel.listDatasetParents: must raise an exception if no parameter is passed"""
        try: self.api.list('datasetparents')
        except: pass
        else: self.fail("Exception was expected and was not raised")
            
    def test24(self):
        """Test24 web.DBSReaderModel.listOutputConfigs: basic test"""
        self.api.list('outputconfigurations')
    
    def test25(self):
        """Test25 web.DBSReaderModel.listOutputConfigs: basic test"""
        self.api.list('outputconfigurations', dataset="*")

    def test26(self):
        """Test26 web.DBSReaderModel.listOutputConfigs: basic test"""
        self.api.list('outputconfigurations', logical_file_name="*")

    def test27(self):
        """Test27 web.DBSReaderModel.listOutputConfigs: basic test"""
        self.api.list('outputconfigurations', release_version="*")

    def test28(self):
        """Test28 web.DBSReaderModel.listOutputConfigs: basic test"""
        self.api.list('outputconfigurations', pset_hash="*")

    def test29(self):
	"""Test29 web.DBSReaderModel.listOutputConfigs: basic test"""
	self.api.list('outputconfigurations', app_name="*")

    def test30(self):
        """Test30 web.DBSReaderModel.listOutputConfigs: basic test"""
	self.api.list('outputconfigurations', output_module_label="*")
 
    def test31(self):
        """Test31 web.DBSReaderModel.listOutputConfigs: basic test"""
	self.api.list('outputconfigurations', dataset="*",
                                              logical_file_name="*",
                                              release_version="*",
                                              pset_hash="*",
                                              app_name="*",
                                              output_module_label="*")

    def test32(self):
        """Test32 web.DBSReaderModel.listFileParents: basic test"""
        self.api.list('fileparents', logical_file_name="*")
    
    def test33(self):
        """Test33 web.DBSReaderModel.listFileParents: must raise an exception if no parameter is passed"""
        try: self.api.list('fileparents')
        except: pass
        else: self.fail("Exception was expected and was not raised")
        
    def test34(self):
        """Test34 web.DBSReaderModel.listFileLumis: basic test"""
        self.api.list('filelumis', logical_file_name="*")

    def test35(self):
        """Test35 web.DBSReaderModel.listFileLumis: basic test"""
        self.api.list('filelumis', block_name="*")

    def test36(self):
        """Test36 web.DBSReaderModel.listFileLumis: must raise an exception if no parameter is passed"""
        try: self.api.list('filelumis')
        except: pass
        else: self.fail("Exception was expected and was not raised")
        
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSReaderModel_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
        
