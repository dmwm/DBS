#!/usr/bin/env python
"""
This module provides MigrationRequests.List data access object.
"""
__revision__ = "$Id: List.py,v 1.2 2010/07/09 14:41:00 afaq Exp $"
__version__ = "$Revision: 1.2 $"


from WMCore.Database.DBFormatter import DBFormatter

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
        self.sql = """SELECT MIGRATION_BLOCK_ID, MIGRATION_REQUEST_ID, MIGRATION_BLOCK_NAME, MIGRATION_ORDER, 
			MIGRATION_STATUS, CREATE_BY, CREATION_DATE, LAST_MODIFIED_BY, LAST_MODIFICATION_DATE 
			FROM %s MIGRATION_BLOCKS 
			WHERE MIGRATION_REQUEST_ID=:migration_request_id AND MIGRATION_STATUS='PENDING' ORDER BY MIGRATION_ORDER DESC
			""" % (self.owner)

    def execute(self, conn, migration_request_id="", transaction=False):
        """
        Lists all primary datasets if pattern is not provided.
        """
        binds = { "migration_request_id" : migration_request_id }
	cursors = self.dbi.processData(self.sql, binds, conn, transaction, returnCursor=True)
        result = self.formatCursor(cursors[0])
        return result
