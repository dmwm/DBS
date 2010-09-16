#!/usr/bin/env python
""" DAO Object for Sites table """ 

__revision__ = "$Revision: 1.1 $"
__version__  = "$Id: Insert.py,v 1.1 2010/02/05 21:00:51 afaq Exp $ "

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


