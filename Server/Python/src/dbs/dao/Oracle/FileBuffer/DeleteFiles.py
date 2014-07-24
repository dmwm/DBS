#!/usr/bin/env python
"""
This module provides FileBuffer.DeleteFiles data access object.
"""
from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class DeleteFiles(DBFormatter):
    """
    File List DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
	self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else "" 
        self.sql = """DELETE FROM %sFILE_BUFFERS WHERE LOGICAL_FILE_NAME=:logical_file_name""" % self.owner

    def execute(self, conn, logical_file_name={}, transaction=False):
        """
	simple execute
        """
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed","Oracle/FileBuffer/DeleteFiles. Expects db connection from upper layer.")

        self.dbi.processData(self.sql, logical_file_name, conn, transaction, returnCursor=True)

