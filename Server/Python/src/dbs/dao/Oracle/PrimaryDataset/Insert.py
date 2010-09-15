#!/usr/bin/env python
""" DAO Object for PrimaryDatasets table """ 

__revision__ = "$Revision: 1.10 $"
__version__  = "$Id: Insert.py,v 1.10 2009/12/23 15:42:05 yuyi Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy import exceptions
from dbs.dao.Oracle.InsertTable.Insert import InsertSingle

class Insert(InsertSingle):
    """ PrimaryDataset Insert DAO Class"""
    def execute(self, daoinput, conn = None, transaction = False):
	try:
	    self.executeSingle(daoinput, "PRIMARY_DATASETS", conn = None, transaction = False)
	except Exception:
	    raise
