#!/usr/bin/env python
"""
This module provides MigrationBlocks.List data access object.
"""
from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class List(DBFormatter):
    """
    MigrationRequest List DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.logger = logger
        self.sql = """SELECT MIGRATION_BLOCK_ID, MIGRATION_REQUEST_ID, MIGRATION_BLOCK_NAME, MIGRATION_ORDER, 
			MIGRATION_STATUS, CREATE_BY, CREATION_DATE, LAST_MODIFIED_BY, LAST_MODIFICATION_DATE 
			FROM %sMIGRATION_BLOCKS 
			WHERE MIGRATION_REQUEST_ID=:migration_request_id AND (MIGRATION_STATUS=0 or MIGRATION_STATUS=3) ORDER BY MIGRATION_ORDER DESC
			""" % (self.owner)

    def execute(self, conn, migration_request_id="", transaction=False):
        """
        Lists all primary datasets if pattern is not provided.
        """
        if not conn:
	    dbsExceptionHandler("dbsException-failed-connect2host", "Oracle/MigrationBlock/List. Expects db connection from upper layer.", self.logger.exception )

        binds = { "migration_request_id" : migration_request_id }
	cursors = self.dbi.processData(self.sql, binds, conn, transaction, returnCursor=True)
        result = []
        for c in cursors:
            result.extend(self.formatCursor(c))
        return result
