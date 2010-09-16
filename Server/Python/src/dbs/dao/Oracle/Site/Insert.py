#!/usr/bin/env python
""" DAO Object for Sites table """ 

__revision__ = "$Revision: 1.4 $"
__version__  = "$Id: Insert.py,v 1.4 2010/01/28 22:54:02 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy import exceptions
from dbs.dao.Oracle.InsertTable.Insert import InsertSingle

class Insert(InsertSingle):
    """ DAO for Insert Site """
    def execute( self, daoinput, conn=None, transaction=False ):
	try:
            self.executeSingle(daoinput, "SITES", conn, transaction)
        except Exception:
            raise


