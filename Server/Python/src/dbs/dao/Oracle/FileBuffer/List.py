#!/usr/bin/env python
"""
This module provides File.List data access object.
"""
from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class List(DBFormatter):
    """
    File List DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
	self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else "" 
        self.sql = """SELECT FILE_CLOB FROM %sFILE_BUFFERS WHERE BLOCK_ID=:block_id AND rownum < 10""" % self.owner

    def execute(self, conn, block_id="", transaction=False):
        """
	simple execute
        """
        if not conn:
            dbsExceptionHandler("dbsException-db-conn-failed", "Oracle/FileBuffer/List. Expects db connection from upper layer.")

	sql = self.sql
        binds = { "block_id" : block_id}
        cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
        result = self.formatCursor(cursors[0])
        return result 

