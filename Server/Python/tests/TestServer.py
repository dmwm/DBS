"""This module provides all basic tests for the server"""

__revision__ = "$Id: TestServer.py,v 1.4 2009/11/03 16:42:25 akhukhun Exp $"
__version__ = "$Revision: 1.4 $"

INSERTCOUNT = "3005"

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
        self.cli.get("primds?primds=*bla*")
    
    def test02(self):
        """listDatasets"""
        self.cli.get("datasets")
        self.cli.get("datasets/*")
        self.cli.get("datasets/a/b")
        self.cli.get("datasets/a/*/c")
        self.cli.get("datasets?primds=a&tier=*")
        
    def test03(self):
        """insertPrimaryDataset"""
        dinput = {"primarydsname":"ServerUTPrimaryDataset_" + INSERTCOUNT,
                  "primarydstype":"mc"}
        self.cli.put("primds", dinput)
        
    def test04(self):
        """insertBlock"""
        dinput = {"blockname":"/RelValQCD_Pt_80_120/CMSSW_3_1_3-MC_31X_V5-v1/GEN-SIM-RECO#inertedbyserverunittest_" + INSERTCOUNT,
                  "openforwriting":True,
                  "originsite":"FNAL",
                  "blocksize":345,
                  "filecount":20
                  }
        self.cli.put("blocks", dinput)

        
    def test05(self):
        """insertDataset"""
        dinput = {"dataset":"/RelValQCD_Pt_80_120/CMSSW_3_1_3-MC_31X_V5-v%s/GEN-SIM-RECO" % INSERTCOUNT,
                  "isdatasetvalid":True,
                  "datasettype":"PRODUCTION",
                  "acquisitionera":"TODAY",
                  "processingversion":"TEST",
                  "physicsgroup":"Individual",
                  "xtcrosssection":234.,
                  "globaltag":"GLOBALTAG"}
        self.cli.put("datasets", dinput)
    
    def test06(self):
        "listBlocks"
        self.cli.get("blocks/a/b/c/d")

    def test07(self):
        "listFiles"
        self.cli.get("files/a/b/c")
        self.cli.get("files/a/b/c/d")
        self.cli.get("files/a/b/c/d/*e")
        self.cli.get("files?lfn=*")
        
    def test08(self):
        "insertFile"
        sinput = []
        for k in range(100):
            file = {"logicalfilename":"sfile_"+INSERTCOUNT+str(k) + ".root",
                    "isfilevalid":True,
                    "block":"/RelValQCD_Pt_80_120/CMSSW_3_1_3-MC_31X_V5-v1/GEN-SIM-RECO#2015",
                    "filetype":"EDM",
                    "checksum":"999",
                    "eventcount":10000,
                    "filesize":1024.,
                    "branchhash":"branchhash",
                    "adler32":"adler32",
                    "md5":"md5",
                    "autocrosssection":1234.}
            sinput.append(file)
        self.cli.put("files", sinput)

if __name__ == "__main__":
    
    #SUITE = unittest.TestLoader().loadTestsFromTestCase(TestServer)
    SUITE = unittest.TestSuite()
    SUITE.addTest(TestServer("test08"))
    unittest.TextTestRunner(verbosity=2).run(SUITE)