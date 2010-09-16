#!/usr/bin/env python
""" DAO Object for Sites table """ 

__revision__ = "$Revision: 1.3 $"
__version__  = "$Id: Insert.py,v 1.3 2009/12/23 20:45:47 yuyi Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy import exceptions
from dbs.dao.Oracle.InsertTable.Insert import InsertSingle

class Insert(InsertSingle):
    """ DAO for Insert Site """
    def execute( self, daoinput, conn=None, transaction=False ):
	try:
            self.executeSingle(daoinput, "SITES", conn = None, transaction = False)
        except Exception:
            raise


