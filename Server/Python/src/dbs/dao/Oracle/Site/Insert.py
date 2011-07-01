#!/usr/bin/env python
""" DAO Object for Sites table """ 

__revision__ = "$Revision: 1.7 $"
__version__  = "$Id: Insert.py,v 1.7 2010/06/23 21:21:26 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.dao.Oracle.InsertTable.Insert import InsertSingle

from sqlalchemy import exceptions

class Insert(InsertSingle):
    """ DAO for Insert Site """
    def execute( self, conn, daoinput, transaction=False ):
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed","Oracle/Site/Insert. Expects db connection from upper layer.")

        self.executeSingle(conn, daoinput, "SITES", transaction)
        

