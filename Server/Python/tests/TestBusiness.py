"""This module provides all bascs tests for the business objects."""

__revision__ = "$Id: TestBusiness.py,v 1.4 2009/11/03 16:42:25 akhukhun Exp $"
__version__ = "$Revision: 1.4 $"

import unittest
import logging
from WMCore.Database.DBFactory import DBFactory

INSERTCOUNT = "2027"

class TestBusiness(unittest.TestCase):
    """Business layer unittests class"""
    def setUp(self):
        """setup all necessary parameters"""
        #url = "oracle://cms_dbs_afaq:anzpw03062009@oradev10.cern.ch:10520/D10"
        url = "oracle://CMS_DBS3_OWNER:new4_dbs3@uscmsdb03.fnal.gov:1521/cmscald"
        self.logger = logging.getLogger("dbs test logger")
        self.dbi = DBFactory(self.logger, url).connect()
        
    def test01(self):
        """DBSPrimaryDataset.insertPrimaryDataset"""
        from dbs.business.DBSPrimaryDataset import DBSPrimaryDataset
        bo = DBSPrimaryDataset(self.logger, self.dbi)
        binput = {"primarydsname":"QCD_700_" + INSERTCOUNT,
                 "primarydstype":"mc",
                 "creationdate":1234,
                 "createby":"akhukhun@cern.ch"}
        bo.insertPrimaryDataset(binput)

    def test02(self):
        """DBSPrimaryDataset.listPrimaryDatasets"""
        from dbs.business.DBSPrimaryDataset import DBSPrimaryDataset
        bo = DBSPrimaryDataset(self.logger, self.dbi)
        bo.listPrimaryDatasets()
        result = bo.listPrimaryDatasets("ThisDoesNotExist")
        self.assertEqual(len(result), 0)
        
    def test03(self):
        """DBSDataset.insertDataset"""
        from dbs.business.DBSDataset import DBSDataset
        bo = DBSDataset(self.logger, self.dbi)
        binput = {"dataset":"/RelValQCD_Pt_80_120/CMSSW_3_1_3-MC_31X_V5-v%s/GEN-SIM-RECO" % INSERTCOUNT,
                  "isdatasetvalid":1,
                  "primaryds":"RelValQCD_Pt_80_120",
                  "processedds":"CMSSW_3_1_3-MC_31X_V5-v%s" % INSERTCOUNT,
                  "datatier":"GEN-SIM-RECO",
                  "datasettype":"PRODUCTION",
                  "acquisitionera":"TODAY",
                  "processingversion":"TEST",
                  "physicsgroup":"Individual",
                  "xtcrosssection":999.,
                  "globaltag":"DaoTESTGlobalTag",
                  "creationdate":1234,
                  "createby":"me",
                  "lastmodificationdate":1235,
                  "lastmodifiedby":"alsome"}                  
        bo.insertDataset(binput)
    
    def test04(self):
        """DBSDataset.listDatasets"""
        from dbs.business.DBSDataset import DBSDataset
        bo = DBSDataset(self.logger, self.dbi)
        bo.listDatasets()
        bo.listDatasets("a")
        bo.listDatasets("a","%b")
        bo.listDatasets("a", "b","c%")
        bo.listDatasets(primdsname = "a", datatiername = "c", procdsname = "vax")

    def test05(self):
        """DBSBLock.insertBlock"""
        from dbs.business.DBSBlock import DBSBlock
        bo = DBSBlock(self.logger, self.dbi)
        binput = {"blockname":"/RelValQCD_Pt_80_120/CMSSW_3_1_3-MC_31X_V5-v1/GEN-SIM-RECO#" + INSERTCOUNT,
                  "dataset":"/RelValQCD_Pt_80_120/CMSSW_3_1_3-MC_31X_V5-v1/GEN-SIM-RECO",
                  "openforwriting":True,
                  "originsite":"FNAL",
                  "blocksize":9999,
                  "filecount":1000,
                  "creationdate":123,
                  "createby":"akhukhun@cern.ch",
                  "lastmodificationdate":1234,
                  "lastmodifiedby":"ak427@cornell.edu"}
        
        bo.insertBlock(binput)
        
        
    def test06(self):
        """DBSBlock.listBlocks"""
        from dbs.business.DBSBlock import DBSBlock
        bo = DBSBlock(self.logger, self.dbi)
        print bo.listBlocks("/RelValQCD_Pt_80_120/CMSSW_3_1_3-MC_31X_V5-v1/GEN-SIM-RECO")
        print bo.listBlocks("/RelValQCD_Pt_80_120/CMSSW_3_1_3-MC_31X_V5-v1/GEN-SIM-RECO",
                            "/RelValQCD_Pt_80_120/CMSSW_3_1_3-MC_31X_V5-v1/GEN-SIM-RECO#%20%")
        
    def test07(self):
        """DBSFile.listFiles"""
        from dbs.business.DBSFile import DBSFile 
        bo = DBSFile(self.logger, self.dbi)
        bo.listFiles("/RelValQCD_Pt_80_120/CMSSW_3_1_3-MC_31X_V5-v1/GEN-SIM-RECO")
        bo.listFiles(block = "/RelValQCD_Pt_80_120/CMSSW_3_1_3-MC_31X_V5-v1/GEN-SIM-RECO#b110ad98-ab46-4f56-ad7c-ce762f2450c7")
        
    def test08(self):
        """DBSFile.insertFile"""
        from dbs.business.DBSFile import DBSFile
        bo = DBSFile(self.logger, self.dbi)
        binput = []
        for k in range(100):
            file = {"logicalfilename":"bfile_" + INSERTCOUNT + str(k) + ".root",
                    "isfilevalid":True,
                    "dataset":"/RelValQCD_Pt_80_120/CMSSW_3_1_3-MC_31X_V5-v1/GEN-SIM-RECO",
                    "block":"/RelValQCD_Pt_80_120/CMSSW_3_1_3-MC_31X_V5-v1/GEN-SIM-RECO#2015",
                    "filetype":"EDM",
                    "checksum":"999",
                    "eventcount":1000,
                    "filesize":1024,
                    "branchhash":"branchhash",
                    "adler32":"adler32",
                    "md5":"md5",
                    "autocrosssection":12345.,
                    "creationdate":1234,
                    "createby":"aleko@cornell.edu",
                    "lastmodificationdate":12345,
                    "lastmodifiedby":"aleko@cornell.edu"}
            binput.append(file)
        bo.insertFile(binput)
        
        
        
if __name__ == "__main__":
    
    #SUITE = unittest.TestLoader().loadTestsFromTestCase(TestBusiness)
    SUITE = unittest.TestSuite()
    SUITE.addTest(TestBusiness("test08"))
    
    unittest.TextTestRunner(verbosity=2).run(SUITE)

