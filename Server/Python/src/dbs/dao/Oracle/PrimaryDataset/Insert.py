#!/usr/bin/env python
""" DAO Object for PrimaryDatasets table """ 

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsException import dbsException, dbsExceptionCode
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.dao.Oracle.InsertTable.Insert import InsertSingle

class Insert(InsertSingle):
    """ PrimaryDataset Insert DAO Class"""
    def execute(self, conn, daoinput, transaction = False):
        self.executeSingle(conn, daoinput, "PRIMARY_DATASETS", transaction )
