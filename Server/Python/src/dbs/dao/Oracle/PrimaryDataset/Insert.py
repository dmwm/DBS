#!/usr/bin/env python
""" DAO Object for PrimaryDatasets table """ 

__revision__ = "$Revision: 1.14 $"
__version__  = "$Id: Insert.py,v 1.14 2010/06/23 21:21:25 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy import exceptions
from dbs.utils.dbsException import dbsException,dbsExceptionCode
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.dao.Oracle.InsertTable.Insert import InsertSingle

class Insert(InsertSingle):
    """ PrimaryDataset Insert DAO Class"""
    def execute(self, conn, daoinput, transaction = False):
	if not conn:
	    dbsExceptionHandler("dbsException-dao", "PrimaryDataset/Insert expects db connection from upper layer.")
        self.executeSingle(conn, daoinput, "PRIMARY_DATASETS", transaction )
