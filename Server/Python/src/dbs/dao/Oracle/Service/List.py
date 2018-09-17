#!/usr/bin/env python
"""
This module provides Services.List data access object.
"""
from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class List(DBFormatter):
    """
    Services List DAO class.
    """

    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
	
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = """
	SELECT SERVICE_ID, NAME, TYPE, LOCATION, 
	    STATUS, ADMIN, URI, DB, VERSION, 
	    LAST_CONTACT, ALIAS, COMMENTS 
	    FROM %sSERVICES """ % (self.owner)

    def execute(self, conn, transaction=False):
        """
        Lists all primary datasets if pattern is not provided.
        """
        sql = self.sql
        binds = {}
        
	cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
        result = []
        for c in cursors:
            result.extend(self.formatCursor(c, size=100))
        return result
