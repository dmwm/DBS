#!/usr/bin/env python
""" DAO Object for ProcessingEras table """ 

__revision__ = "$Revision: 1.6 $"
__version__  = "$Id: Insert.py,v 1.6 2010/06/23 21:21:26 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from dbs.dao.Oracle.InsertTable.Insert import InsertSingle
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Insert(InsertSingle):

    def __init__(self, logger, dbi, owner):
            DBFormatter.__init__(self, logger, dbi)
            self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""

    def execute( self, conn, binds, transaction=False ):
	if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed","Oracle/ProcessingEra/Insert. Expects db connection from upper layer.")

        self.executeSingle(conn, binds, "PROCESSING_ERAS", transaction)
        
	return
