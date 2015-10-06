#!/usr/bin/env python
"""
This module provides MigrationRequests.List data access object.
"""
__revision__ = "$Id: FindMigrateableBlocks.py,v 1.3 2010/08/18 21:18:22 yuyi Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class FindMigrateableBlocks(DBFormatter):
    """
    MigrationRequest FindMigrateableBlocks DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = """SELECT MIGRATION_BLOCK_ID, MIGRATION_BLOCK_NAME, MIGRATION_ORDER 
			FROM %s MIGRATION_BLOCKS 
			WHERE MIGRATION_REQUEST_ID=:migration_request_id AND MIGRATION_STATUS= 0 ORDER BY MIGRATION_ORDER DESC
			""" % (self.owner)

    def execute(self, conn, migration_request_id="", transaction=False):
        """
        Lists all primary datasets if pattern is not provided.
        """
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed", "Oracle/MigrationBlocks/FindMigrateableBlocks. Expects db connection from upper layer.")

        binds = { "migration_request_id" : migration_request_id }
	cursors = self.dbi.processData(self.sql, binds, conn, transaction, returnCursor=True)
        result = self.formatCursor(cursors[0])
        return result
