#!/usr/bin/env python
"""
This module provides File.List data access object.
"""
from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class ListBlocks(DBFormatter):
    """
    File List DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
	self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else "" 
        self.sql = """SELECT DISTINCT BLOCK_ID from %sFILE_BUFFERS""" % self.owner

    def execute(self, conn, transaction=False):

        """
	simple execute
        """
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed", "Oracle/FileBuffer/ListBlocks. Expects db connection from upper layer.")
        
        binds = {}
        cursors = self.dbi.processData(self.sql, binds, conn, transaction, returnCursor=True)
        result = self.formatCursor(cursors[0])
        return result 

