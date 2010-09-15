# DAO Object for ApplicationExecutable table
# $Revision: 1.1 $
# $Id: Insert.py,v 1.1 2009/10/12 16:48:23 afaq Exp $

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    sql = """INSERT INTO APPLICATION_EXECUTABLES(APP_EXEC_ID, APP_NAME) VALUES (:appexecid, :appname);"""

    def getBinds( self, application_executablesObj ):
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


    def execute( self, application_executablesObj ):
            binds = self.getBinds(application_executablesObj )
            result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
            return