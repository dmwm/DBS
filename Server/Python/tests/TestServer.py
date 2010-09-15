"""This module provides all basic tests for the server"""

__revision__ = "$Id: TestServer.py,v 1.1 2009/10/28 14:53:07 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"

INSERTCOUNT = "02"

import json
import unittest
from DBS3SimpleClient import DBS3Client

class TestServer(unittest.TestCase):
    """Server unittests class"""
    def setUp(self):
        """setup all necessary parameters"""
        url = "http://localhost:8585/dbs3/"
        self.cli = DBS3Client(url)
        
    def test01(self):
        """listPrimaryDatasets"""
        self.cli.get("primds")
        self.cli.get("primds/*")
        self.cli.get("primds?primdsname=*bla*")
    
    def test02(self):
        """listDatasets"""
        self.cli.get("datasets")
        self.cli.get("datasets/*")
        self.cli.get("datasets/a/b")
        self.cli.get("datasets/a/*/c")
        self.cli.get("datasets?primdsname=a&datatiername=*")
        
    def test03(self):
        """insertPrimaryDataset"""
        dinput = {"primarydsname":"ServerUTPrimaryDataset_" + INSERTCOUNT,
                  "primarydstype":"mc"}
        self.cli.put("primds", dinput)
        
    def test04(self):
        """insertBlock"""
        dinput = {"blockname":"/RelValQCD_Pt_80_120/CMSSW_3_1_3-MC_31X_V5-v1/GEN-SIM-RECO#inertedbyserverunittest_" + INSERTCOUNT,
                  "openforwriting":1,
                  "sitname":"alekossite",
                  "blocksize":345,
                  "filecount":20
                  }

if __name__ == "__main__":
    """    
    SUITE = unittest.TestSuite()
    SUITE.addTest(TestBusiness("test01"))
    SUITE.addTest(TestBusiness("test02"))
    SUITE.addTest(TestBusiness("test03"))
    SUITE.addTest(TestBusiness("test04"))
    #unittest.TextTestRunner(verbosity=2).run(SUITE)
    """
    SUITE = unittest.TestLoader().loadTestsFromTestCase(TestServer)
    unittest.TextTestRunner(verbosity=2).run(SUITE)