#!/usr/bin/env python
""" DAO Object for Sites table """ 

__revision__ = "$Revision: 1.5 $"
__version__  = "$Id: Insert.py,v 1.5 2010/03/05 20:12:34 yuyi Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy import exceptions
from dbs.dao.Oracle.InsertTable.Insert import InsertSingle

class Insert(InsertSingle):
    """ DAO for Insert Site """
    def execute( self, conn, daoinput, transaction=False ):
	if not conn:
	    raise Exception("dbs/dao/Oracle//Insert expects db connection from up layer.")
	try:
            self.executeSingle(daoinput, "SITES", conn, transaction)
        except Exception:
            raise


