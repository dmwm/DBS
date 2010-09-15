"""This module provides all bascs tests for the business objects."""

__revision__ = "$Id: TestBusiness.py,v 1.3 2009/10/30 16:56:01 akhukhun Exp $"
__version__ = "$Revision: 1.3 $"

import unittest
import logging
from WMCore.Database.DBFactory import DBFactory

INSERTCOUNT = "2023"

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
        """DBSDataset.Insert"""
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
        """DBSBLock.Insert"""
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
        """DBSBlock.List"""
        from dbs.business.DBSBlock import DBSBlock
        bo = DBSBlock(self.logger, self.dbi)
        print bo.listBlocks("/RelValQCD_Pt_80_120/CMSSW_3_1_3-MC_31X_V5-v1/GEN-SIM-RECO")
        print bo.listBlocks("/RelValQCD_Pt_80_120/CMSSW_3_1_3-MC_31X_V5-v1/GEN-SIM-RECO",
                            "/RelValQCD_Pt_80_120/CMSSW_3_1_3-MC_31X_V5-v1/GEN-SIM-RECO#%20%")
        
        
if __name__ == "__main__":
    
    SUITE = unittest.TestLoader().loadTestsFromTestCase(TestBusiness)
    unittest.TextTestRunner(verbosity=2).run(SUITE)

