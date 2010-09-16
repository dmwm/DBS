#!/usr/bin/env python
"""
This module provides MigrationRequests.List data access object.
"""
__revision__ = "$Id: List.py,v 1.1 2010/04/22 07:53:14 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"


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
        self.sql = \
"""
SELECT MR.MIGRATION_ID, MR.MIGRATION_URL, 
       MR.MIGRATION_DATASET, MR.MIGRATION_STATUS
FROM %sMIGRATION_REQUESTS MR
""" % (self.owner)

    def execute(self, conn, migration_dataset, transaction=False):
        """
        Lists all primary datasets if pattern is not provided.
        """
        sql = self.sql
        binds = {}
        op = ("=", "like")["%" in migration_dataset]
        sql += "WHERE MR.MIGRATION_DATASET %s :migration_dataset" % op 
        binds = {'migration_dataset':migration_dataset}
	cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
	assert len(cursors) == 1
        result = self.formatCursor(cursors[0])
        return result
