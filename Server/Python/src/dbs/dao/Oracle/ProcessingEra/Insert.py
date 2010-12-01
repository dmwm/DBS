#!/usr/bin/env python
""" DAO Object for ProcessingEras table """ 

__revision__ = "$Revision: 1.6 $"
__version__  = "$Id: Insert.py,v 1.6 2010/06/23 21:21:26 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from dbs.dao.Oracle.InsertTable.Insert import InsertSingle

class Insert(InsertSingle):

    def __init__(self, logger, dbi, owner):
            DBFormatter.__init__(self, logger, dbi)
            self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""

    def execute( self, conn, binds, transaction=False ):
        if not conn:
            raise Exception("dbs/dao/Oracle/ProcessingEra/Insert expects db connection from upper layer.")
        try:
            self.executeSingle(conn, binds, "PROCESSING_ERAS", transaction)
        except Exception:
            raise 
	return
