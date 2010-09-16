#!/usr/bin/env python
""" DAO Object for PrimaryDatasets table """ 

__revision__ = "$Revision: 1.11 $"
__version__  = "$Id: Insert.py,v 1.11 2010/01/28 22:54:02 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy import exceptions
from dbs.dao.Oracle.InsertTable.Insert import InsertSingle

class Insert(InsertSingle):
    """ PrimaryDataset Insert DAO Class"""
    def execute(self, daoinput, conn = None, transaction = False):
	try:
	    self.executeSingle(daoinput, "PRIMARY_DATASETS", conn , transaction )
	except Exception:
	    raise
