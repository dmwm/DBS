#!/usr/bin/env python
"""
This module provides Block.ListChild data access object.
"""
from WMCore.Database.DBFormatter import DBFormatter
from WMCore.Database.MySQLCore import  MySQLInterface
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class ListChild(DBFormatter):
    """
    Block List DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.logger = logger
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = """SELECT BC.BLOCK_NAME FROM %sBLOCKS BC
			JOIN %sBLOCK_PARENTS BPRTS
			    ON BPRTS.THIS_BLOCK_ID = BC.BLOCK_ID
			JOIN %sBLOCKS BP
			    ON BPRTS.PARENT_BLOCK_ID = BP.BLOCK_ID
		      WHERE BP.BLOCK_NAME = :block_name""" % ((self.owner,)*3)

    def execute(self, conn, block_name="", transaction = False):
        """
        block: /a/b/c#d
        """	
        if not conn:
            msg='Oracle/BlockParent/List. No DB connection found'
            dbsExceptionHandler('dbsException-failed-connect2host', msg, self.logger.exception)

        sql = self.sql
        binds = {}
	if block_name:
	    binds.update(block_name = block_name)
        else:
            dbsExceptionHandler("dbsException-invalid-input", "Oracle/BlockParent/ListChild. block_name must be provided.", self.logger.exception)

	cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
        result = []
        for c in cursors:
            result.extend(self.formatCursor(c, size=100))
        return result
