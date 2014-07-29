#!/usr/bin/env python
""" DAO Object for DbsVersions table """ 

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""

            self.sql = """INSERT INTO %sDBS_VERSIONS ( DBS_VERSION_ID, SCHEMA_VERSION, DBS_RELEASE_VERSION, INSTANCE_NAME, INSTANCE_TYPE, CREATION_DATE, LAST_MODIFICATION_DATE) VALUES (:dbsversionid, :schemaversion, :dbsreleaseversion, :instancename, :instancetype, :creationdate, :lastmodificationdate)""" % (self.owner)

    def getBinds_delme( self, dbs_versionsObj ):
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


    def execute( self, conn, dbs_versionsObj, transaction=False ):
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed","Oracle/DbsVersion/Insert. Expects db connection from upper layer.")
        
	result = self.dbi.processData(self.sql, binds, conn, transaction)
	return


