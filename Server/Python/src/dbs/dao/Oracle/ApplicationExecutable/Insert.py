#!/usr/bin/env python
""" DAO Object for ApplicationExecutables table """ 

__revision__ = "$Revision: 1.3 $"
__version__  = "$Id: Insert.py,v 1.3 2009/12/21 21:05:38 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner

            self.sql = """INSERT INTO %sAPPLICATION_EXECUTABLES ( APP_EXEC_ID, APP_NAME) VALUES (:app_exec_id, :app_name)""" % (self.owner)

    def execute( self, appExeObj, conn=None, transaction=False ):
            ##binds = self.getBinds( application_executablesObj )
            result = self.dbi.processData(self.sql, appExeObj, conn, transaction)
            return


