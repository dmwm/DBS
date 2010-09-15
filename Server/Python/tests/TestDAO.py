"""This module provides all basic tests for the dao objects
As the number of tests increase we will probably repackage these tests
to separate package/modules for each DAO object
"""

__revision__ = "$Id: TestDAO.py,v 1.1 2009/10/27 17:28:05 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"

import unittest
import logging
from WMCore.Database.DBFactory import DBFactory


class TestDAO(unittest.TestCase):
    """DAO unittests class"""
    def setUp(self):
        """setup all necessary variables"""
        url = "oracle://cms_dbs_afaq:anzpw03062009@oradev10.cern.ch:10520/D10"
        self.logger = logging.getLogger("dbs test logger")
        self.dbi = DBFactory(self.logger, url).connect()
        
    def test01(self):
        """PrimaryDSType.List"""
        from dbs.dao.Oracle.PrimaryDSType.List import List as PrimaryDSTypeList
        dao = PrimaryDSTypeList(self.logger, self.dbi)
        dao.execute()
        result = dao.execute("ThisDoesNotExist")
        self.assertEqual(len(result), 0)

    def test02(self):
        """PrimaryDataset.Insert"""
        from dbs.dao.Oracle.PrimaryDataset.Insert import Insert as PrimaryDatasetInsert
        dao = PrimaryDatasetInsert(self.logger, self.dbi)
        dinput = {"primarydsid":407,
                 "primarydsname":"QCD_100_407",
                 "primarydstypeid":1,
                 "creationdate":1234,
                 "createby":"akhukhunATcern.ch"}
        dao.execute(dinput)

    def test03(self):
        """PrimaryDataset.List"""
        from dbs.dao.Oracle.PrimaryDataset.List import List as PrimaryDatasetList
        dao = PrimaryDatasetList(self.logger, self.dbi)
        dao.execute()
        result = dao.execute("ThisDoesNotExist")
        self.assertEqual(len(result), 0)


    def test04(self):
        """Block.Insert"""
        from dbs.dao.Oracle.Block.Insert import Insert as BlockInsert
        dao = BlockInsert(self.logger, self.dbi)
        dinput = {"blockid":15,
                 "blockname":"115",
                 "datasetid":1,
                 "openforwriting":True,
                 "originsite":1,
                 "blocksize":1024,
                 "filecount":10,
                 "creationdate":12345,
                 "createby":"akhukhunATcern.ch",
                 "lastmodificationdate":12345,
                 "lastmodifiedby":"akhukhunATcern.ch"
                }
        dao.execute(dinput)
    
    def test05(self):
        """Site.List"""
        from dbs.dao.Oracle.Site.List import List as SiteList
        dao = SiteList(self.logger, self.dbi)
        dao.execute()
        result = dao.execute("ThisDoesNotExist")
        self.assertEqual(len(result), 0) 
        
    def test06(self):
        """Dataset.List"""
        from dbs.dao.Oracle.Dataset.List import List as DatasetList
        dao = DatasetList(self.logger, self.dbi)
        dao.execute()
        dao.execute("a")
        dao.execute("a", "b%", "c")
        
        
        
        
if __name__ == "__main__":
    
    #unittest.main()

    SUITE = unittest.TestSuite()
    SUITE.addTest(TestDAO("test01"))
    #SUITE.addTest(TestDAO("test02"))
    SUITE.addTest(TestDAO("test03"))
    #SUITE.addTest(TestDAO("test04"))
    SUITE.addTest(TestDAO("test05"))
    SUITE.addTest(TestDAO("test06"))
    unittest.TextTestRunner(verbosity=2).run(SUITE)
