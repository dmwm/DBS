"""
web unittests
"""

__revision__ = "$Id: DBSWriterModel_t.py,v 1.4 2010/01/14 16:53:24 afaq Exp $"
__version__ = "$Revision: 1.4 $"

import os
import unittest
from dbsserver_t.utils.DBSRestApi import DBSRestApi
from ctypes import *

"""
This is has to be change everytime running the test. So we need to make it using uuid. YG 1/11/10
COUNTER = os.environ['DBS_TEST_COUNTER']
"""
class DBSWriterModel_t(unittest.TestCase):

    self.uid = self.uuid()
    self.primary_ds_name = 'unittest_web_primary_ds_name_%s' % uid
    self.dataset = 'unittest_web_dataset_%s' % uid 
    
    def uuid(self):
	lib = CDLL("libuuid.so.1")
	uuid = create_string_buffer(16)
	return lib.uuid_generate(byref(uuid))
	
    def setUp(self):
        """setup all necessary parameters"""
        config = os.environ["DBS_TEST_CONFIG_WRITER"] 
        self.api = DBSRestApi(config) 

    def test01(self):
        """web.DBSReaderModel.insertPrimaryDataset: basic test"""
	COUNTER = self.uuid()
	#import pdb
	#pdb.set_trace()
        data = {'primary_ds_name':self.primary_ds_name,
                'primary_ds_type':'TEST'}
        self.api.insert('primarydatasets', data)

    def test02(self):
        """web.DBSReaderModel.insertDataset: basic test"""
        #import pdb
        #pdb.set_trace()
        data = {'primary_ds_name':self.primary_ds_name,
                'dataset':self.dataset}
        self.api.insert('datasets', data)
	
    def test03(self):
	"""web.DBSReaderModel.insertPrimaryDataset: missing 'primary_ds_name, must throw exception"""
	data = {'primary_ds_type':'TEST'}
	try:
	    self.api.insert('primarydatasets', data)
	except:
	     pass
	else:
	     self.fail("Exception was expected and was not raised.")

     
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSWriterModel_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
