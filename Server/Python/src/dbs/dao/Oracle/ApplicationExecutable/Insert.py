#!/usr/bin/env python
""" DAO Object for ApplicationExecutables table """ 

__revision__ = "$Revision: 1.4 $"
__version__  = "$Id: Insert.py,v 1.4 2010/01/11 22:56:43 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner

            self.sql = """INSERT INTO %sAPPLICATION_EXECUTABLES ( APP_EXEC_ID, APP_NAME) VALUES (:app_exec_id, :app_name)""" % (self.owner)

    def execute( self, appExeObj, conn=None, transaction=False ):
	try:
            result = self.dbi.processData(self.sql, appExeObj, conn, transaction)
 	except Exception, e:
	    if str(ex).lower().find("unique constraint") != -1 :
		pass
	    else:
		raise


