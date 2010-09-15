# DAO Object for Site table
# $Revision: 1.1 $
# $Id: Insert.py,v 1.1 2009/10/12 16:48:32 afaq Exp $

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    sql = """INSERT INTO SITES(SITE_ID, SITE_NAME) VALUES (:siteid, :sitename);"""

    def getBinds( self, sitesObj ):
            binds = {}
            if type(sitesObj) == type ('object'):
            	binds = {
			'siteid' : sitesObj['siteid'],
			'sitename' : sitesObj['sitename'],
                 }

            elif type(sitesObj) == type([]):
               binds = []
               for item in sitesObj:
                   binds.append({
 	                'siteid' : item['siteid'],
 	                'sitename' : item['sitename'],
 	                })
               return binds


    def execute( self, sitesObj ):
            binds = self.getBinds(sitesObj )
            result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
            return