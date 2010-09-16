"""This module provides all basic tests for the server"""

__revision__ = "$Id: TestStress.py,v 1.1 2009/11/19 17:38:57 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"

import threading
from DBS3SimpleClient import DBS3Client
from threading import Thread

class DBS3StressTest(Thread):
    def __init__(self, url = "http://localhost:8585/dbs3/", IC = 0, NFILES = 1, NLUMIS = 0):
        Thread.__init__(self)
        self.cli = DBS3Client(url)
        self.IC = IC
        self.NFILES = NFILES
        self.NLUMIS = NLUMIS
        
    def run(self):
        """insert one Primary Dataset, one Dataset, one Block, 
        nfiles Files and one file with nfiles parents and nlumis lumi sections"""
        
        #insert primary dataset
        dinput = {"PRIMARY_DS_NAME":"SUT_%s" % self.IC,
                  "PRIMARY_DS_TYPE":"TEST"}
        self.cli.put("primarydatasets", dinput)
        print "%s: INSERT PRIMARY DATASET" % self.IC
 
        #insertDataset
        dinput = {"PRIMARY_DS_NAME":"SUT_%s" % self.IC,
                  "PROCESSED_DATASET_NAME":"SUT_PROCESSED_DATASET_V%s" % self.IC,
                  "DATA_TIER_NAME":"GEN-SIM-RECO",
                  "DATASET":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO" % (self.IC, self.IC),
                  "IS_DATASET_VALID":1,
                  "DATASET_TYPE":"PRODUCTION",
                  #"acquisitionera":"TEST",
                  #"processingversion":"TEST",
                  "PHYSICS_GROUP_NAME":"Individual",
                  "XTCROSSSECTION":234.,
                  "GLOBAL_TAG":"GLOBALTAG"}
        self.cli.put("datasets", dinput)
        print "%s: INSERT DATASET" % self.IC
 
        #insertBlock
        dinput = {"BLOCK_NAME":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO#SUT_BLOCK_%s" % (self.IC, self.IC, self.IC),
                  "blocksize":345,
                  "filecount":20
                  }
        self.cli.put("blocks", dinput)
        print "%s: INSERT BLOCK" % self.IC
 
        #insertFile
        sinput = []
        for k in range(self.NFILES):
            file = {"LOGICAL_FILE_NAME":"/store/sut_file_%s_%s.root" % (self.IC, str(k)),
                    "IS_FILE_VALID":True,
                    "BLOCK":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO#SUT_BLOCK_%s" % (self.IC, self.IC, self.IC),
                    "FILE_TYPE":"EDM",
                    "CHECK_SUM":"999",
                    "EVENT_COUNT":10000,
                    "FILE_SIZE":1024.,
                    "ADLER32":"adler32",
                    "MD5":"md5",
                    "AUTO_CROSS_SECTION":1234.}
            sinput.append(file)
        self.cli.put("files", sinput)
        print "%s: INSERT FILES" % self.IC
        
        #insert one File with Lumi Sections and file parents
        lumilist = []
        parentlist = []
        for l in range(self.NLUMIS):
            lumilist.append({"RUN_NUM":1000*int(self.IC)+l,
                             "LUMI_SECTION_NUM":10000*int(self.IC)+l})
        for p in range(self.NFILES):
            parentlist.append({"FILE_PARENT_LFN":"/store/sut_file_%s_%s.root" % (self.IC, str(p))})
        
        sinput = {"LOGICAL_FILE_NAME":"/store/sut_file_withlumisandparents_%s.root" % self.IC,
                    "IS_FILE_VALID":True,
                    "BLOCK":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO#SUT_BLOCK_%s" % (self.IC, self.IC, self.IC),
                    "FILE_TYPE":"EDM",
                    "CHECK_SUM":"999",
                    "EVENT_COUNT":10000,
                    "FILE_SIZE":1024.,
                    "ADLER32":"adler32",
                    "MD5":"md5",
                    "AUTO_CROSS_SECTION":1234.,
                    "FILE_LUMI_LIST":lumilist,
                    "FILE_PARENT_LIST":parentlist}
        self.cli.put("files", sinput)
        print "%s: INSERT FILE WITH PARENTS AND LUMIS" % self.IC
        
        
        #listPrimaryDatasets
        self.cli.get("primarydatasets")
        res = self.cli.get("primarydatasets/SUT_%s" % self.IC)
        print "%s: LIST PRIMARY DATASETS" % self.IC
    
        #listDatasets
        self.cli.get("datasets")
        params = {"dataset":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO" % (self.IC, self.IC)}
        res = self.cli.get("datasets", params)
        print "%s: LIST DATASETS" % self.IC
        
        
        #listBlocks
        params = {"dataset":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO" % (self.IC, self.IC)}
        res = self.cli.get("blocks", params)
        params = {"block":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO#SUT_BLOCK_%s" % (self.IC, self.IC, self.IC)}
        res = self.cli.get("blocks", params)
        print "%s: LIST BLOCKS" % self.IC

        #listFiles
        params = {"block":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO#SUT_BLOCK_%s" % (self.IC, self.IC, self.IC)}
        res = self.cli.get("files", params)
        print "%s: LIST FILES" % self.IC
        
if __name__ == "__main__":
    for i in range(1, 100):
        current = DBS3StressTest(IC = i, NFILES = 150, NLUMIS = 1200)
        current.start()
    
    
