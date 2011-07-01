#!/usr/bin/env python
""" DAO Object for ApplicationExecutables table """ 

__revision__ = "$Revision: 1.10 $"
__version__  = "$Id: Insert.py,v 1.10 2010/06/23 21:21:18 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""

            self.sql = """INSERT INTO %sAPPLICATION_EXECUTABLES ( APP_EXEC_ID, APP_NAME) VALUES (:app_exec_id, :app_name)""" % (self.owner)

    def execute( self, conn, appExeObj, transaction=False ):
	if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed", "Oracle/ApplicationExecutable/Insert.  Expects db connection from upper layer.")
        
        result = self.dbi.processData(self.sql, appExeObj, conn, transaction)
 	

