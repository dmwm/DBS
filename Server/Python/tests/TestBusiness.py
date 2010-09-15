"""This module provides all bascs tests for the business objects."""

__revision__ = "$Id: TestBusiness.py,v 1.1 2009/10/27 17:28:05 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"

import unittest
import logging
from WMCore.Database.DBFactory import DBFactory


class TestBusiness(unittest.TestCase):
    def setUp(self):
        url = "oracle://cms_dbs_afaq:anzpw03062009@oradev10.cern.ch:10520/D10"
        self.logger = logging.getLogger("dbs test logger")
        self.dbi = DBFactory(self.logger, url).connect()
        
    def test01(self):
        from dbs.business.DBSPrimaryDataset import DBSPrimaryDataset
        """DBSPrimaryDataset.insertPrimaryDataset"""
        bo = DBSPrimaryDataset(self.logger, self.dbi)
        input = {"primarydsname":"QCD_100_504",
                 "primarydstype":"mc",
                 "creationdate":1234,
                 "createby":"akhukhun@cern.ch"}
        bo.insertPrimaryDataset(input)

    def test02(self):
        from dbs.business.DBSPrimaryDataset import DBSPrimaryDataset
        """DBSPrimaryDataset.listPrimaryDatasets"""
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
        bo.listDatasets("a","b","c%")
        bo.listDatasets(primdsname = "a", datatiername = "c", procdsname = "vax")

if __name__ == "__main__":
    
    #unittest.main()

    suite = unittest.TestSuite()
    suite.addTest(TestBusiness("test01"))
    suite.addTest(TestBusiness("test02"))
    suite.addTest(TestBusiness("test04"))
    unittest.TextTestRunner(verbosity=2).run(suite)
