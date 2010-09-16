"""This module provides all basic tests for the server"""

__revision__ = "$Id: TestServer.py,v 1.8 2009/11/29 11:37:55 akhukhun Exp $"
__version__ = "$Revision: 1.8 $"

IC = "29"
NFILES = 100
NLUMIS = 50



import json
import unittest
from DBS3SimpleClient import DBS3Client


class TestServer(unittest.TestCase):
    """Server unittests class"""
    def setUp(self):
        """setup all necessary parameters"""
        url = "http://localhost:8587/dbs3/"
        self.cli = DBS3Client(url)
        
    def test01(self):
        """insertPrimaryDataset"""
        dinput = {"PRIMARY_DS_NAME":"SUT_%s" % IC,
                  "PRIMARY_DS_TYPE":"TEST"}
        self.cli.put("primarydatasets", dinput)
 
    def test02(self):
        """insertDataset"""
        dinput = {"PRIMARY_DS_NAME":"SUT_%s" % IC,
                  "PROCESSED_DATASET_NAME":"SUT_PROCESSED_DATASET_V%s" % IC,
                  "DATA_TIER_NAME":"GEN-SIM-RECO",
                  "DATASET":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO" % (IC, IC),
                  "IS_DATASET_VALID":1,
                  "DATASET_TYPE":"PRODUCTION",
                  "PHYSICS_GROUP_NAME":"Individual",
                  "XTCROSSSECTION":234.,
                  "GLOBAL_TAG":"GLOBALTAG"}
        self.cli.put("datasets", dinput)
 
    def test03(self):
        """insertBlock"""
        dinput = {"BLOCK_NAME":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO#SUT_BLOCK_%s" % (IC, IC, IC),
                  "blocksize":345,
                  "filecount":20
                  }
        self.cli.put("blocks", dinput)
 
    def test04(self):
        "insertFile"
        sinput = []
        for k in range(NFILES):
            file = {"LOGICAL_FILE_NAME":"/store/sut_file_%s_%s.root" % (IC, str(k)),
                    "IS_FILE_VALID":True,
                    "BLOCK":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO#SUT_BLOCK_%s" % (IC, IC, IC),
                    "DATASET":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO" % (IC, IC),
                    "FILE_TYPE":"EDM",
                    "CHECK_SUM":"999",
                    "EVENT_COUNT":10000,
                    "FILE_SIZE":1024.,
                    "ADLER32":"adler32",
                    "MD5":"md5",
                    "AUTO_CROSS_SECTION":1234.}
            sinput.append(file)
        self.cli.put("files",{"files": sinput})
        
    def test05(self):
        "insert one File with Lumi Sections and file parents"
        lumilist = []
        parentlist = []
        for l in range(NLUMIS):
            lumilist.append({"RUN_NUM":1000*int(IC)+l,
                             "LUMI_SECTION_NUM":10000*int(IC)+l})
        for p in range(NFILES):
            parentlist.append({"FILE_PARENT_LFN":"/store/sut_file_%s_%s.root" % (IC, str(p))})
        
        sinput = {"LOGICAL_FILE_NAME":"/store/sut_file_withlumisandparents_%s.root" % IC,
                    "IS_FILE_VALID":True,
                    "BLOCK":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO#SUT_BLOCK_%s" % (IC, IC, IC),
                    "FILE_TYPE":"EDM",
                    "DATASET":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO" % (IC, IC),
                    "CHECK_SUM":"999",
                    "EVENT_COUNT":10000,
                    "FILE_SIZE":1024.,
                    "ADLER32":"adler32",
                    "MD5":"md5",
                    "AUTO_CROSS_SECTION":1234.,
                    "FILE_LUMI_LIST":lumilist,
                    "FILE_PARENT_LIST":parentlist}
        self.cli.put("files", {"files":sinput})
        
        
    def test10(self):
        """listPrimaryDatasets"""
        self.cli.get("primarydatasets")
        res = self.cli.get("primarydatasets/SUT_%s" % IC)
        self.assertEqual(len(res["result"]), 1)
    
    def test11(self):
        """listDatasets"""
        self.cli.get("datasets")
        params = {"dataset":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO" % (IC, IC)}
        res = self.cli.get("datasets", params)
        self.assertEqual(len(res["result"]), 1)
        
        
    def test12(self):
        "listBlocks"
        params = {"dataset":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO" % (IC, IC)}
        res = self.cli.get("blocks", params)
        self.assertEqual(len(res["result"]), 1)
        params = {"block":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO#SUT_BLOCK_%s" % (IC, IC, IC)}
        res = self.cli.get("blocks", params)
        self.assertEqual(len(res["result"]), 1)

    def test13(self):
        "listFiles"
        #params = {"dataset":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO" % (IC, IC)}
        #res = self.cli.get("files", params)
        #self.assertEqual(len(res["result"]), NFILES+1)
        
        params = {"block":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO#SUT_BLOCK_%s" % (IC, IC, IC)}
        res = self.cli.get("files", params)
        self.assertEqual(len(res["result"]), NFILES+2)
        
if __name__ == "__main__":
    
    SUITE = unittest.TestLoader().loadTestsFromTestCase(TestServer)
    #SUITE = unittest.TestSuite()
    #SUITE.addTest(TestServer("test04"))
    unittest.TextTestRunner(verbosity=2).run(SUITE)
