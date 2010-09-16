"""This module provides all basic tests for the server"""

__revision__ = "$Id: TestStress.py,v 1.3 2009/11/29 11:37:54 akhukhun Exp $"
__version__ = "$Revision: 1.3 $"

import threading
from DBS3SimpleClient import DBS3Client
from threading import Thread
import time

class DBS3StressTest(Thread):
    def __init__(self, url = "http://localhost:8585/dbs3/", IC = 0, NFILES = 1, NPARENTS = 0, NLUMIS = 0):
        Thread.__init__(self)
        self.cli = DBS3Client(url)
        self.IC = IC
        self.NFILES = NFILES
        self.NPARENTS = NPARENTS
        self.NLUMIS = NLUMIS
        
    def run(self):
        """insert one Primary Dataset, one Dataset, one Block, 
        nfiles Files and one file with nfiles parents and nlumis lumi sections"""
        t0 = time.time()        
        
        #insert primary dataset
        dinput = {"PRIMARY_DS_NAME":"SUT_%s" % self.IC,
                  "PRIMARY_DS_TYPE":"TEST"}
        t = time.time()
        self.cli.put("primarydatasets", dinput)
        print "%s: INSERT PRIMARY DATASET:    %s" % (self.IC, time.time() - t)
 
        #insertDataset
        dinput = {"PRIMARY_DS_NAME":"SUT_%s" % self.IC,
                  "PROCESSED_DATASET_NAME":"SUT_PROCESSED_DATASET_V%s" % self.IC,
                  "DATA_TIER_NAME":"GEN-SIM-RECO",
                  "DATASET":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO" % (self.IC, self.IC),
                  "IS_DATASET_VALID":1,
                  "PRIMARY_DS_TYPE":"TEST",
                  "DATASET_TYPE":"PRODUCTION",
                  "ACQUISITION_ERA_NAME":"TEST",
                  "PROCESSING_VERSION":"TEST",
                  "PHYSICS_GROUP_NAME":"Individual",
                  "XTCROSSSECTION":234.,
                  "GLOBAL_TAG":"GLOBALTAG"}
        t = time.time()
        self.cli.put("datasets", dinput)
        print "%s: INSERT DATASET :    %s" % (self.IC, time.time() - t)
 
        #insertBlock
        dinput = {"BLOCK_NAME":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO#SUT_BLOCK_%s" % (self.IC, self.IC, self.IC),
                  "BLOCK_SIZE":345,
                  "FILE_COUNT":20,
                  "OPEN_FOR_WRITING":1
                  }
        t = time.time()
        self.cli.put("blocks", dinput)
        print "%s: INSERT BLOCK:    %s" % (self.IC, time.time() - t)
 
        #insertFile
        sinput = []
        for k in range(self.NPARENTS):
            fl = {"LOGICAL_FILE_NAME":"/store/sut_file_%s_%s.root" % (self.IC, str(k)),
                    "IS_FILE_VALID":1,
                    "BLOCK":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO#SUT_BLOCK_%s" % (self.IC, self.IC, self.IC),
                    "DATASET":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO" % (self.IC, self.IC),
                    "FILE_TYPE":"EDM",
                    "CHECK_SUM":"999",
                    "EVENT_COUNT":10000,
                    "FILE_SIZE":1024.,
                    "ADLER32":"adler32",
                    "MD5":"md5",
                    "AUTO_CROSS_SECTION":1234.,
                    "FILE_LUMI_LIST":[],
                    "FILE_PARENT_LIST":[],}
            sinput.append(fl)
        t = time.time()
        self.cli.put("files", {"files":sinput})
        print "%s: INSERT FILES:    %s" % (self.IC, time.time() - t)
        
        #insert one File with Lumi Sections and file parents
        lumilist = []
        parentlist = []
        for l in range(self.NLUMIS):
            lumilist.append({"RUN_NUM":1000*int(self.IC)+l,
                             "LUMI_SECTION_NUM":10000*int(self.IC)+l})
        for p in range(self.NPARENTS):
            parentlist.append({"FILE_PARENT_LFN":"/store/sut_file_%s_%s.root" % (self.IC, str(p))})
        
        sinput = []
        for f in range(self.NFILES):
            fl = {"LOGICAL_FILE_NAME":"/store/sut_file_withlumisandparents_%s_%s.root" % (self.IC, str(f)),
                    "IS_FILE_VALID":1,
                    "BLOCK":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO#SUT_BLOCK_%s" % (self.IC, self.IC, self.IC),
                    "DATASET":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO" % (self.IC, self.IC),
                    "FILE_TYPE":"EDM",
                    "CHECK_SUM":"999",
                    "EVENT_COUNT":10000,
                    "FILE_SIZE":1024.,
                    "ADLER32":"adler32",
                    "MD5":"md5",
                    "AUTO_CROSS_SECTION":1234.,
                    "FILE_LUMI_LIST":lumilist,
                    "FILE_PARENT_LIST":parentlist}
            sinput.append(fl)
        t = time.time()
        self.cli.put("files", {"files":sinput})
        print "%s: INSERT FILE WITH P&L:    %s" % (self.IC, time.time() - t)
        
        
        #listPrimaryDatasets
        t = time.time()
        self.cli.get("primarydatasets")
        res = self.cli.get("primarydatasets/SUT_%s" % self.IC)
        print "%s: LIST PRIMARY DATASETS:    %s" % (self.IC, time.time() - t)
    
        #listDatasets
        t = time.time()
        self.cli.get("datasets")
        params = {"dataset":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO" % (self.IC, self.IC)}
        res = self.cli.get("datasets", params)
        print "%s: LIST DATASETS:    %s" % (self.IC, time.time() - t)
        
        
        #listBlocks
        t = time.time()
        params = {"dataset":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO" % (self.IC, self.IC)}
        res = self.cli.get("blocks", params)
        params = {"block":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO#SUT_BLOCK_%s" % (self.IC, self.IC, self.IC)}
        res = self.cli.get("blocks", params)
        print "%s: LIST BLOCKS:    %s" % (self.IC, time.time() - t)

        #listFiles
        t = time.time()
        params = {"block":"/SUT_%s/SUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO#SUT_BLOCK_%s" % (self.IC, self.IC, self.IC)}
        res = self.cli.get("files", params)
        print "%s: LIST FILES:    %s" % (self.IC, time.time() - t)

        print "%s: TEST FINISHED in %s SECONDS" % (self.IC, time.time() - t0)
        
if __name__ == "__main__":
    for i in range(200, 210):
        current = DBS3StressTest(url="http://localhost:8587/dbs3/", IC = i, NFILES = 10, NPARENTS = 10, NLUMIS = 10)
        #current = DBS3StressTest(url="http://vocms09.cern.ch:8989/DBSServlet/", IC = i, NFILES = 10000, NPARENTS = 5, NLUMIS = 10)
        current.start()
