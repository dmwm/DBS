#!/usr/bin/env python
""" DAO Object for Sites table """ 

__revision__ = "$Revision: 1.7 $"
__version__  = "$Id: Insert.py,v 1.7 2010/06/23 21:21:26 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy import exceptions
from dbs.dao.Oracle.InsertTable.Insert import InsertSingle

class Insert(InsertSingle):
    """ DAO for Insert Site """
    def execute( self, conn, daoinput, transaction=False ):
	if not conn:
	    raise Exception("dbs/dao/Oracle//Insert expects db connection from upper layer.")
	try:
            self.executeSingle(conn, daoinput, "SITES", transaction)
        except Exception:
            raise


