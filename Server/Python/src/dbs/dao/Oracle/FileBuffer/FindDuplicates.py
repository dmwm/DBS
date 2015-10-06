#!/usr/bin/env python
"""
This module provides FileBuffer.FindDuplicates data access object.
"""
from __future__ import print_function
from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class FindDuplicates(DBFormatter):
    """
    File List DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
	self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else "" 
        self.sql = """SELECT FLBUF.LOGICAL_FILE_NAME FROM %sFILE_BUFFERS FLBUF JOIN %sFILES FL ON FL.LOGICAL_FILE_NAME=FLBUF.LOGICAL_FILE_NAME""" % (2*(self.owner,))

    def execute(self, conn, transaction=False):
        """
	simple execute
        """	
	binds={}
	if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed", "Oracle/FileBuffer/FindDuplicates. Expects db connection from upper layer.")

	print(self.sql)
        cursors=self.dbi.processData(self.sql, binds, conn, transaction, returnCursor=True)
        result = self.formatCursor(cursors[0])
        return result

