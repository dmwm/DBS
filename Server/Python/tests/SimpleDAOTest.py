import logging
from WMCore.Database.DBFactory import DBFactory

from dbs.dao.Oracle.PrimaryDataset.Insert import Insert as PriInsert 
from dbs.dao.Oracle.FileParentBlock import List as FPBList

class Test:

    def __init__(self):
        #url = "oracle://user:password@host:port/sid"
	url="oracle://anzar:anzar_cms2009@uscmsdb03.fnal.gov:1521/cmscald"
        self.logger = logging.getLogger("dbs test logger")
        self.dbi = DBFactory(self.logger, url).connect()

    def testPrimaryInsert(self):
	"""
	This method is being used for testing primary datasets's insert DAO
	"""

	dao = PriInsert(self.logger, self.dbi, "anzar")
	dinput = { "JUNK" : "XYZ" , "primary_ds_id":1001, "primary_ds_name": "TkCosmics38T", "primary_ds_type_id": 1, "creation_date": 1234, "create_by":"anzar"}
        dao.execute(dinput)

    def testFileParentBlock(self):
	"""
	Test listing of File Parent Blocks (and datasets) used by insertFiles API
	"""
	dao = FPBList.List(self.logger, self.dbi, "anzar")
	ret=dao.execute(file_id=13841)
	print ret

    
test=Test()
#test.testPrimaryInsert()
test.testFileParentBlock()
