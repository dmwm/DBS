#!/usr/bin/env python
""" DAO Object for PrimaryDatasets table """ 

__revision__ = "$Revision: 1.12 $"
__version__  = "$Id: Insert.py,v 1.12 2010/03/05 19:23:15 yuyi Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy import exceptions
from dbs.dao.Oracle.InsertTable.Insert import InsertSingle

class Insert(InsertSingle):
    """ PrimaryDataset Insert DAO Class"""
    def execute(self, conn, daoinput, transaction = False):
	if not conn:
	    raise Exception("dbs/dao/Oracle/PrimaryDataset/Insert expects db connection from up layer.")
	try:
	    self.executeSingle(daoinput, "PRIMARY_DATASETS", conn , transaction )
	except Exception:
	    raise
