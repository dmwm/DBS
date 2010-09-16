#!/usr/bin/env python
""" DAO Object for ACQUISITION_ERAS table """

__revision__ = "$Revision: 1.7 $"
__version__  = "$Id: Insert.py,v 1.7 2010/03/05 14:57:52 yuyi Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy import exceptions
from dbs.dao.Oracle.InsertTable.Insert import InsertSingle

class Insert(InsertSingle):
    """ ACQUISITION_ERAS Insert DAO Class"""
    def execute(self, daoinput, conn, transaction = False):
	if not conn:
	    raise Exception("dbs/dao/Oarcle?AcquisitionEra/Insert expects db connection from up layer.")
        try:
            self.executeSingle(daoinput, "ACQUISITION_ERAS", conn, transaction)
        except Exception:
            raise

