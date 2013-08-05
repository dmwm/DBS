#!/usr/bin/env python
"""
This module provides Migration.Remove data access object.
"""

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Remove(DBFormatter):
    """
    Migration Update DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
	self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
"""Delete from %sMIGRATION_REQUESTS  
WHERE MIGRATION_REQUEST_ID=:migration_request_id and (migration_status=0 or migration_status=3)""" %  self.owner 
        
    def execute(self, conn, daoinput, transaction = False):
        """
	    daoinput keys:
	    migration_request_id
        """
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed","Oracle/MigrationRequests/Remove. Expects db connection from upper layer.")
        result = self.dbi.processData(self.sql, daoinput, conn, transaction)
        if result[0].rowcount == 0:
            dbsExceptionHandler('dbsException-invalid-input2',"DBSMigration: Invalid request. Sucessfully processed or processing requests cannot be removed. ")
