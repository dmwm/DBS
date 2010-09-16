#!/usr/bin/env python
""" DAO Object for StorageElements table """ 

__revision__ = "$Revision: 1.5 $"
__version__  = "$Id: Insert.py,v 1.5 2010/03/05 20:17:42 yuyi Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""

            self.sql = """INSERT INTO %sSTORAGE_ELEMENTS ( SE_ID, SE_NAME) VALUES (:seid, :sename)""" % (self.owner)
    def execute( self, conn, storage_elementsObj, transaction=False ):
	if not conn:
	    raise Exception("dbs/dao/Oracle/StorageElement/Insert expects db connection from up layer.")
	result = self.dbi.processData(self.sql,  storage_elementsObj, conn, transaction)
	return


