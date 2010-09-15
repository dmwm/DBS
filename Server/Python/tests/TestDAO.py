"""This module provides all basic tests for the dao objects
As the number of tests increase we will probably repackage these tests
to separate package/modules for each DAO object
"""

__revision__ = "$Id: TestDAO.py,v 1.2 2009/10/30 16:56:30 akhukhun Exp $"
__version__ = "$Revision: 1.2 $"

INSERTCOUNT = "1023"

import unittest
import logging
from WMCore.Database.DBFactory import DBFactory

class TestDAO(unittest.TestCase):
    """DAO unittests class"""
    def setUp(self):
        """setup all necessary variables"""
        #url = "oracle://cms_dbs_afaq:anzpw03062009@oradev10.cern.ch:10520/D10"
        url = "oracle://CMS_DBS3_OWNER:new4_dbs3@uscmsdb03.fnal.gov:1521/cmscald"
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
        dinput = {"primarydsid":INSERTCOUNT,
                 "primarydsname":"QCD_100_" + INSERTCOUNT,
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
        dinput = {"blockid":int(INSERTCOUNT),
                 "blockname":INSERTCOUNT,
                 "dataset":1,
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
        
    def test07(self):
        """AcquisitionEra.Insert"""
        from dbs.dao.Oracle.AcquisitionEra.Insert import Insert as AcqusitionEraInsert
        dao = AcqusitionEraInsert(self.logger, self.dbi)
        dinput = {"acquisitioneraid":int(INSERTCOUNT),
                  "acquisitioneraname":"Ihavenoideahowitshouldlooklike" + INSERTCOUNT,
                  "creationdate":1234,
                  "createby":"me",
                  "description":"nodescription"
                  }
        dao.execute(dinput)
        
    def test08(self):
        """ProcessedDataset.Insert"""
        from dbs.dao.Oracle.ProcessedDataset.Insert import Insert as ProcessedDatasetInsert
        dao = ProcessedDatasetInsert(self.logger, self.dbi)
        dinput = {"processeddsid":int(INSERTCOUNT),
                  "processeddsname":"ProcessedDSName" + INSERTCOUNT}
        dao.execute(dinput)
        
    def test09(self):
        """Dataset.Insert""" 
        from dbs.dao.Oracle.Dataset.Insert import Insert as DatasetInsert
        dao = DatasetInsert(self.logger, self.dbi)
        dinput = {"datasetid":int(INSERTCOUNT),
                 "dataset":"/RelValQCD_Pt_80_120/CMSSW_3_1_3-MC_31X_V5-v%s/GEN-SIM-RECO" % INSERTCOUNT,
                 "isdatasetvalid":1,
                 "primaryds":48,
                 "processedds":1,
                 "datatier":23,
                 "datasettype":1,
                 "acquisitionera":1,
                 "processingversion":1,
                 "physicsgroup":1,
                 "xtcrosssection":999.,
                 "globaltag":"DaoTESTGlobalTag",
                 "creationdate":1234,
                 "createby":"me",
                 "lastmodificationdate":1235,
                 "lastmodifiedby":"alsome"}
       
        dao.execute(dinput)
        
    def test10(self):
        """Block.List"""
        from dbs.dao.Oracle.Block.List import List as BlockList
        dao = BlockList(self.logger, self.dbi)
        dao.execute("/RelValQCD_Pt_80_120/CMSSW_3_1_3-MC_31X_V5-v1/GEN-SIM-RECO")
        dao.execute("/a/b/c/", "/a/b/c#d")
       
if __name__ == "__main__":
    
    SUITE = unittest.TestLoader().loadTestsFromTestCase(TestDAO)
    unittest.TextTestRunner(verbosity=2).run(SUITE)

