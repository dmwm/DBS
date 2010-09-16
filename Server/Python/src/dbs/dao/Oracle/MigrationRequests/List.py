#!/usr/bin/env python
"""
This module provides MigrationRequests.List data access object.
"""
__revision__ = "$Id: List.py,v 1.4 2010/06/28 16:09:04 afaq Exp $"
__version__ = "$Revision: 1.4 $"


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
SELECT MR.MIGRATION_REQUEST_ID, MR.MIGRATION_URL, 
       MR.MIGRATION_INPUT, MR.MIGRATION_STATUS,
       MR.CREATE_BY, MR.CREATION_DATE,
       MR.LAST_MODIFIED_BY, MR.LAST_MODIFICATION_DATE
FROM %sMIGRATION_REQUESTS MR
""" % (self.owner)

    def execute(self, conn, migration_url="", migration_input="", create_by="", migration_request_id="", transaction=False):
        """
        Lists all primary datasets if pattern is not provided.
        """
        sql = self.sql
        binds = {}
	if migration_request_id:
	    sql += " WHERE MR.MIGRATION_REQUEST_ID=:migration_request_id"
	    binds['migration_request_id']=migration_request_id
	else:    
	    if  migration_url or migration_input or create_by:
		sql += " WHERE "
	    if migration_url:
		sql += " MR.MIGRATION_URL=:migration_url"
		binds['migration_url']=migration_url
	    if  migration_input:
		if migration_url:
		    sql += " AND "
		op = ("=", "like")["%" in migration_input]
		sql += " MR.MIGRATION_INPUT %s :migration_input" % op
		binds['migration_input']=migration_input
	    if create_by:
		if  migration_url or migration_input:
		    sql += " AND "
		sql += " MR.CREATE_BY=:create_by" %create_by
		binds['create_by']=create_by
	cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
        result = self.formatCursor(cursors[0])
        return result
