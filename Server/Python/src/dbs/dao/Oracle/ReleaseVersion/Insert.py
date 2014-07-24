#!/usr/bin/env python
""" DAO Object for ReleaseVersions table """ 

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""

            self.sql = """INSERT INTO %sRELEASE_VERSIONS ( RELEASE_VERSION_ID, RELEASE_VERSION) VALUES (:release_version_id, :release_version)""" % (self.owner)

    def execute( self, conn, relVerObj, transaction=False ):
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed","Oracle/ReleaseVersion/Insert. Expects db connection from upper layer.")

        result = self.dbi.processData(self.sql, relVerObj, conn, transaction)
 	
