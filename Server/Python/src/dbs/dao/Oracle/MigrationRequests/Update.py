#!/usr/bin/env python
"""
This module provides Migration.Update data access object.
"""

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Update(DBFormatter):
    """
    Migration Update DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.logger = logger
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
"""UPDATE %sMIGRATION_REQUESTS 
SET MIGRATION_STATUS=:migration_status 
WHERE MIGRATION_REQUEST_ID=:migration_request_id""" %  self.owner 
        
    def execute(self, conn, daoinput, transaction = False):
        """
	    daoinput keys:
	    migration_status, migration_request_id
        """
        if not conn:
	    dbsExceptionHandler("dbsException-failed-connect2host", "Oracle/MigrationRequests/Update. Expects db connection from upper layer.",
                                self.logger.exception)

        result = self.dbi.processData(self.sql, daoinput, conn, transaction)
