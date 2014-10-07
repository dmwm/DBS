#!/usr/bin/env python
""" DAO Object for Sites table """ 

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.dao.Oracle.InsertTable.Insert import InsertSingle

class Insert(InsertSingle):
    """ DAO for Insert Site """
    def execute( self, conn, daoinput, transaction=False ):
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed","Oracle/Site/Insert. Expects db connection from upper layer.")

        self.executeSingle(conn, daoinput, "SITES", transaction)
        

