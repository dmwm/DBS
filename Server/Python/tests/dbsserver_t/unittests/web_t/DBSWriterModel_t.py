"""
web unittests
"""

__revision__ = "$Id: DBSWriterModel_t.py,v 1.2 2010/01/11 18:02:47 yuyi Exp $"
__version__ = "$Revision: 1.2 $"

import os
import unittest
from dbsserver_t.utils.DBSRestApi import DBSRestApi
from ctypes import *

"""
This is has to be change everytime running the test. So we need to make it using uuid. YG 1/11/10
COUNTER = os.environ['DBS_TEST_COUNTER']
"""
class DBSWriterModel_t(unittest.TestCase):
    
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
	import pdb
	pdb.set_trace()
        data = {'primary_ds_name':'unittest_web_primary_ds_name_%s' % COUNTER,
                'primary_ds_type':'TEST'}
        self.api.insert('primarydatasets', data)
        
        
if __name__ == "__main__":
    SUITE = unittest.TestLoader().loadTestsFromTestCase(DBSWriterModel_t)
    unittest.TextTestRunner(verbosity=2).run(SUITE)
