#!/usr/bin/env python
""" DAO Object for ApplicationExecutables table """ 

__revision__ = "$Revision: 1.2 $"
__version__  = "$Id: Insert.py,v 1.2 2009/10/20 02:19:16 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
            self.owner = "%s." % self.dbi.engine.url.username

            self.sql = """INSERT INTO %sAPPLICATION_EXECUTABLES ( APP_EXEC_ID, APP_NAME) VALUES (:appexecid, :appname) % (self.owner) ;"""

    def getBinds_delme( self, application_executablesObj ):
            binds = {}
            if type(application_executablesObj) == type ('object'):
            	binds = {
			'appexecid' : application_executablesObj['appexecid'],
			'appname' : application_executablesObj['appname'],
                 }

            elif type(application_executablesObj) == type([]):
               binds = []
               for item in application_executablesObj:
                   binds.append({
 	                'appexecid' : item['appexecid'],
 	                'appname' : item['appname'],
 	                })
               return binds


    def execute( self, application_executablesObj, conn=None, transaction=False ):
            ##binds = self.getBinds( application_executablesObj )
            result = self.dbi.processData(self.sql, binds, conn, transaction)
            return


