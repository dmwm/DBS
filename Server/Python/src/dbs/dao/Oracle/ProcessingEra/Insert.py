#!/usr/bin/env python
""" DAO Object for ProcessingEras table """ 

from WMCore.Database.DBFormatter import DBFormatter
from dbs.dao.Oracle.InsertTable.Insert import InsertSingle
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Insert(InsertSingle):

    def __init__(self, logger, dbi, owner):
            DBFormatter.__init__(self, logger, dbi)
            self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""

    def execute( self, conn, binds, transaction=False ):
        self.executeSingle(conn, binds, "PROCESSING_ERAS", transaction)
        
	return
