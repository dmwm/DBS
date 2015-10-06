#!/usr/bin/env python
"""
This module provides MigrationRequests.ListOldest data access object.
"""

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class ListOldest(DBFormatter):
    """
    MigrationRequest ListOldest DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = """SELECT MIN(MIGRATION_REQUEST_ID) AS MIGRATION_REQUEST_ID FROM %sMIGRATION_REQUESTS WHERE MIGRATION_STATUS='PENDING' """ % (self.owner)

    def execute(self, conn, migration_url="", migration_input="", create_by="", migration_request_id="", transaction=False):
        """
        Lists the oldest request queued 
        """
	if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed", "Oracle/MigrationRequests/ListOldest. Expects db connection from upper layer.")

        binds = {}
	result = self.dbi.processData(self.sql, binds, conn, transaction)
        result = self.formatDict(result)
	if len(result) == 0 :
	    return []
	if result[0]["migration_request_id"] in ('', None) :
	    return []
        return result
