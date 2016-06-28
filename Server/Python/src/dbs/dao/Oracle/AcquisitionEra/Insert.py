#!/usr/bin/env python
""" DAO Object for ACQUISITION_ERAS table """

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionDef import DBSEXCEPTIONS
from dbs.dao.Oracle.InsertTable.Insert import InsertSingle

from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Insert(InsertSingle):
    """ ACQUISITION_ERAS Insert DAO Class"""
    def execute(self, conn, daoinput, transaction = False):
        self.executeSingle(conn, daoinput, "ACQUISITION_ERAS", transaction)
        
