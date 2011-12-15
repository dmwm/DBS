#!/usr/bin/env python
""" DAO Object for ACQUISITION_ERAS table """

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionDef import DBSEXCEPTIONS
from sqlalchemy import exceptions
from dbs.dao.Oracle.InsertTable.Insert import InsertSingle

from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Insert(InsertSingle):
    """ ACQUISITION_ERAS Insert DAO Class"""
    def execute(self, conn, daoinput, transaction = False):
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed","Oracle/AcquisitionEra/Insert. Expects db connection from upper layer.")

        self.executeSingle(conn, daoinput, "ACQUISITION_ERAS", transaction)
        
