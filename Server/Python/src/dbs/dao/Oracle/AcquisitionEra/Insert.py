#!/usr/bin/env python
""" DAO Object for ACQUISITION_ERAS table """

__revision__ = "$Revision: 1.6 $"
__version__  = "$Id: Insert.py,v 1.6 2010/01/28 22:54:01 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy import exceptions
from dbs.dao.Oracle.InsertTable.Insert import InsertSingle

class Insert(InsertSingle):
    """ ACQUISITION_ERAS Insert DAO Class"""
    def execute(self, daoinput, conn = None, transaction = False):
        try:
            self.executeSingle(daoinput, "ACQUISITION_ERAS", conn, transaction)
        except Exception:
            raise

