# DAO Object for DbsVersion table
# $Revision: 1.1 $
# $Id: Insert.py,v 1.1 2009/10/12 16:48:27 afaq Exp $

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    sql = """INSERT INTO DBS_VERSIONS(DBS_VERSION_ID, SCHEMA_VERSION, DBS_RELEASE_VERSION, INSTANCE_NAME, INSTANCE_TYPE, CREATION_DATE, LAST_MODIFICATION_DATE) VALUES (:dbsversionid, :schemaversion, :dbsreleaseversion, :instancename, :instancetype, :creationdate, :lastmodificationdate);"""

    def getBinds( self, dbs_versionsObj ):
            binds = {}
            if type(dbs_versionsObj) == type ('object'):
            	binds = {
			'dbsversionid' : dbs_versionsObj['dbsversionid'],
			'schemaversion' : dbs_versionsObj['schemaversion'],
			'dbsreleaseversion' : dbs_versionsObj['dbsreleaseversion'],
			'instancename' : dbs_versionsObj['instancename'],
			'instancetype' : dbs_versionsObj['instancetype'],
			'creationdate' : dbs_versionsObj['creationdate'],
			'lastmodificationdate' : dbs_versionsObj['lastmodificationdate'],
                 }

            elif type(dbs_versionsObj) == type([]):
               binds = []
               for item in dbs_versionsObj:
                   binds.append({
 	                'dbsversionid' : item['dbsversionid'],
 	                'schemaversion' : item['schemaversion'],
 	                'dbsreleaseversion' : item['dbsreleaseversion'],
 	                'instancename' : item['instancename'],
 	                'instancetype' : item['instancetype'],
 	                'creationdate' : item['creationdate'],
 	                'lastmodificationdate' : item['lastmodificationdate'],
 	                })
               return binds


    def execute( self, dbs_versionsObj ):
            binds = self.getBinds(dbs_versionsObj )
            result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
            return