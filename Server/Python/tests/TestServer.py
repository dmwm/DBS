"""This module provides all basic tests for the server"""

__revision__ = "$Id: TestServer.py,v 1.5 2009/11/12 15:21:43 akhukhun Exp $"
__version__ = "$Revision: 1.5 $"

IC = "13"
NFILES = 100

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
        """insertPrimaryDataset"""
        dinput = {"primarydsname":"SUT_%s" % IC,
                  "primarydstype":"TEST"}
        self.cli.put("primarydatasets", dinput)
 
    def test02(self):
        """insertDataset"""
        dinput = {"dataset":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO" % (IC, IC),
                  "isdatasetvalid":True,
                  "datasettype":"PRODUCTION",
                  "acquisitionera":"TEST",
                  "processingversion":"TEST",
                  "physicsgroup":"Individual",
                  "xtcrosssection":234.,
                  "globaltag":"GLOBALTAG"}
        self.cli.put("datasets", dinput)
 
    def test03(self):
        """insertBlock"""
        dinput = {"blockname":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO#SUT_BLOCK_%s" % (IC, IC, IC),
                  "openforwriting":True,
                  "originsite":"TEST",
                  "blocksize":345,
                  "filecount":20
                  }
        self.cli.put("blocks", dinput)
 
    def test04(self):
        "insertFile"
        sinput = []
        for k in range(NFILES):
            file = {"logicalfilename":"/store/sut_file_%s_%s.root" % (IC, str(k)),
                    "isfilevalid":True,
                    "block":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO#SUT_BLOCK_%s" % (IC, IC, IC),
                    "filetype":"EDM",
                    "checksum":"999",
                    "eventcount":10000,
                    "filesize":1024.,
                    "branchhash":"TEST",
                    "adler32":"adler32",
                    "md5":"md5",
                    "autocrosssection":1234.}
            sinput.append(file)
        self.cli.put("files", sinput)
        
    def test05(self):
        """listPrimaryDatasets"""
        self.cli.get("primarydatasets")
        res = self.cli.get("primarydatasets/SUT_%s" % IC)
        self.assertEqual(len(res["result"]), 1)
    
    def test06(self):
        """listDatasets"""
        self.cli.get("datasets")
        params = {"dataset":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO" % (IC, IC)}
        res = self.cli.get("datasets", params)
        self.assertEqual(len(res["result"]), 1)
        
        
    def test07(self):
        "listBlocks"
        params = {"dataset":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO" % (IC, IC)}
        res = self.cli.get("blocks", params)
        self.assertEqual(len(res["result"]), 1)
        params = {"block":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO#SUT_BLOCK_%s" % (IC, IC, IC)}
        res = self.cli.get("blocks", params)
        self.assertEqual(len(res["result"]), 1)

    def test08(self):
        "listFiles"
        #params = {"dataset":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO" % (IC, IC)}
        #res = self.cli.get("files", params)
        #self.assertEqual(len(res["result"]), NFILES+1)
        
        params = {"block":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO#SUT_BLOCK_%s" % (IC, IC, IC)}
        res = self.cli.get("files", params)
        self.assertEqual(len(res["result"]), NFILES+1)
        
if __name__ == "__main__":
    
    SUITE = unittest.TestLoader().loadTestsFromTestCase(TestServer)
    #SUITE = unittest.TestSuite()
    #SUITE.addTest(TestServer("test04"))
    unittest.TextTestRunner(verbosity=2).run(SUITE)