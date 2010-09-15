"""This module provides all bascs tests for the business objects."""

__revision__ = "$Id: TestBusiness.py,v 1.6 2009/11/29 11:37:55 akhukhun Exp $"
__version__ = "$Revision: 1.6 $"

import unittest
import logging
from WMCore.Database.DBFactory import DBFactory

IC = "02"

class TestBusiness(unittest.TestCase):
    """Business layer unittests class"""
    def setUp(self):
        """setup all necessary parameters"""
        url = "oracle://user:password@host:port/sid"
        self.logger = logging.getLogger("dbs test logger")
        self.dbi = DBFactory(self.logger, url).connect()
        
    def test01(self):
        """DBSPrimaryDataset.insertPrimaryDataset"""
        from dbs.business.DBSPrimaryDataset import DBSPrimaryDataset
        bo = DBSPrimaryDataset(self.logger, self.dbi)
        binput = {"primarydsname":"BUT_%s" % IC,
                 "primarydstype":"TEST",
                 "creationdate":1234,
                 "createby":"akhukhun@cern.ch"}
        bo.insertPrimaryDataset(binput)
        
    def test02(self):
        """DBSDataset.insertDataset"""
        from dbs.business.DBSDataset import DBSDataset
        bo = DBSDataset(self.logger, self.dbi)
        binput = {"dataset":"/BUT_%s/BUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO" % (IC, IC),
                  "isdatasetvalid":True,
                  "primaryds":"BUT_%s" % IC,
                  "processedds":"BUT_PROCESSED_DATASET_V%s" % IC,
                  "datatier":"GEN-SIM-RECO",
                  "datasettype":"PRODUCTION",
                  "acquisitionera":"TEST",
                  "processingversion":"TEST",
                  "physicsgroup":"Individual",
                  "xtcrosssection":999.,
                  "globaltag":"DaoTESTGlobalTag",
                  "creationdate":1234,
                  "createby":"me",
                  "lastmodificationdate":1235,
                  "lastmodifiedby":"alsome"}                  
        bo.insertDataset(binput)
        
    def test03(self):
        """DBSBLock.insertBlock"""
        from dbs.business.DBSBlock import DBSBlock
        bo = DBSBlock(self.logger, self.dbi)
        binput = {"blockname":"/BUT_%s/BUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO#BUT_BLOCK_%s" % (IC, IC, IC),
                  "dataset":"/BUT_%s/BUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO" % (IC, IC),
                  "openforwriting":True,
                  "originsite":"TEST",
                  "blocksize":9999,
                  "filecount":1000,
                  "creationdate":123,
                  "createby":"akhukhun@cern.ch",
                  "lastmodificationdate":1234,
                  "lastmodifiedby":"ak427@cornell.edu"}
        
        bo.insertBlock(binput)

    def test04(self):
        """DBSFile.insertFile"""
        from dbs.business.DBSFile import DBSFile
        bo = DBSFile(self.logger, self.dbi)
        binput = []
        for k in range(100):
            file = {"logicalfilename":"/store/but_file_%s_%s.root" % (IC, str(k)),
                    "isfilevalid":True,
                    "dataset":"/BUT_%s/BUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO" % (IC, IC),
                    "block":"/BUT_%s/BUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO#BUT_BLOCK_%s" % (IC, IC, IC),
                    "filetype":"EDM",
                    "checksum":"999",
                    "eventcount":1000,
                    "filesize":1024,
                    "branchhash":"TEST",
                    "adler32":"adler32",
                    "md5":"md5",
                    "autocrosssection":12345.,
                    "creationdate":1234,
                    "createby":"aleko@cornell.edu",
                    "lastmodificationdate":12345,
                    "lastmodifiedby":"aleko@cornell.edu"}
            binput.append(file)
        bo.insertFile(binput)
        
    def test05(self):
        """DBSPrimaryDataset.listPrimaryDatasets"""
        from dbs.business.DBSPrimaryDataset import DBSPrimaryDataset
        bo = DBSPrimaryDataset(self.logger, self.dbi)
        bo.listPrimaryDatasets()
        result = bo.listPrimaryDatasets("ThisDoesNotExist")
        self.assertEqual(len(result), 0)
        
    
    def test06(self):
        """DBSDataset.listDatasets"""
        from dbs.business.DBSDataset import DBSDataset
        bo = DBSDataset(self.logger, self.dbi)
        bo.listDatasets()
        bo.listDatasets("a")
        bo.listDatasets("a","%b")
        bo.listDatasets("a", "b","c%")
        bo.listDatasets(primdsname = "a", datatiername = "c", procdsname = "vax")

        
        
    def test07(self):
        """DBSBlock.listBlocks"""
        from dbs.business.DBSBlock import DBSBlock
        bo = DBSBlock(self.logger, self.dbi)
        bo.listBlocks("/BUT_%s/BUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO" % (IC, IC))
        
    def test08(self):
        """DBSFile.listFiles"""
        from dbs.business.DBSFile import DBSFile 
        bo = DBSFile(self.logger, self.dbi)
        bo.listFiles("/BUT_%s/BUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO" % (IC, IC))
        bo.listFiles(block = "/BUT_%s/BUT_PROCESSED_DATASET_V%s/GEN-SIM-RECO#BUT_BLOCK_%s" % (IC, IC, IC))
        
        
        
        
if __name__ == "__main__":
    
    SUITE = unittest.TestLoader().loadTestsFromTestCase(TestBusiness)
    #SUITE = unittest.TestSuite()
    #SUITE.addTest(TestBusiness("test08"))
    
    unittest.TextTestRunner(verbosity=2).run(SUITE)

