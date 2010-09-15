"""This module provides all bascs tests for the business objects."""

__revision__ = "$Id: TestBusiness.py,v 1.2 2009/10/28 09:54:18 akhukhun Exp $"
__version__ = "$Revision: 1.2 $"

import unittest
import logging
from WMCore.Database.DBFactory import DBFactory


class TestBusiness(unittest.TestCase):
    """Business layer unittests class"""
    def setUp(self):
        """setup all necessary parameters"""
        url = "oracle://cms_dbs_afaq:anzpw03062009@oradev10.cern.ch:10520/D10"
        self.logger = logging.getLogger("dbs test logger")
        self.dbi = DBFactory(self.logger, url).connect()
        
    def test01(self):
        """DBSPrimaryDataset.insertPrimaryDataset"""
        from dbs.business.DBSPrimaryDataset import DBSPrimaryDataset
        bo = DBSPrimaryDataset(self.logger, self.dbi)
        binput = {"primarydsname":"QCD_100_509",
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
        binput = {"blockname":"/RelValQCD_Pt_80_120/CMSSW_3_1_3-MC_31X_V5-v1/GEN-SIM-RECO#1234567",
                  "dataset":"/RelValQCD_Pt_80_120/CMSSW_3_1_3-MC_31X_V5-v1/GEN-SIM-RECO",
                  "openforwriting":1,
                  "sitename":"alekossite",
                  "blocksize":9999,
                  "filecount":1000,
                  "creationdate":123,
                  "createby":"akhukhun@cern.ch",
                  "lastmodificationdate":1234,
                  "lastmodifiedby":"ak427@cornell.edu"}
        
        bo.insertBlock(binput)
        
if __name__ == "__main__":
    
    #unittest.main()

    SUITE = unittest.TestSuite()
    SUITE.addTest(TestBusiness("test01"))
    SUITE.addTest(TestBusiness("test02"))
    SUITE.addTest(TestBusiness("test04"))
    SUITE.addTest(TestBusiness("test05"))
    unittest.TextTestRunner(verbosity=2).run(SUITE)
