# DAO Object for ReleaseVersion table
# $Revision: 1.1 $
# $Id: Insert.py,v 1.1 2009/10/12 16:48:31 afaq Exp $

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    sql = """INSERT INTO RELEASE_VERSIONS(RELEASE_VERSION_ID, VERSION) VALUES (:releaseversionid, :version);"""

    def getBinds( self, release_versionsObj ):
            binds = {}
            if type(release_versionsObj) == type ('object'):
            	binds = {
			'releaseversionid' : release_versionsObj['releaseversionid'],
			'version' : release_versionsObj['version'],
                 }

            elif type(release_versionsObj) == type([]):
               binds = []
               for item in release_versionsObj:
                   binds.append({
 	                'releaseversionid' : item['releaseversionid'],
 	                'version' : item['version'],
 	                })
               return binds


    def execute( self, release_versionsObj ):
            binds = self.getBinds(release_versionsObj )
            result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
            return