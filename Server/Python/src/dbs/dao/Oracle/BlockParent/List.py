#!/usr/bin/env python
"""
This module provides Block.List data access object.
"""
__revision__ = "$Id: List.py,v 1.2 2010/06/23 21:21:18 afaq Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.Database.MySQLCore import  MySQLInterface
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class List(DBFormatter):
    """
    Block List DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = """SELECT BC.BLOCK_NAME as THIS_BLOCK_NAME, BP.BLOCK_NAME as PARENT_BLOCK_NAME FROM %sBLOCKS BP
			JOIN %sBLOCK_PARENTS BPRTS
			    ON BPRTS.PARENT_BLOCK_ID = BP.BLOCK_ID
			JOIN %sBLOCKS BC
			    ON BPRTS.THIS_BLOCK_ID = BC.BLOCK_ID
		      WHERE BC.BLOCK_NAME = :block_name""" % ((self.owner,)*3)

    def execute(self, conn, block_name="", transaction = False):
        """
        block: /a/b/c#d
        """
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed","Oracle/BlockParent/List. Expects db connection from upper layer.")

        sql = self.sql
        
	if type(block_name) is str:
	    binds = {'block_name' :block_name}
        elif type(block_name) is list:
            binds = [{'block_name':x} for x in block_name]
        else: 
            msg = "Oracle/BlockParent/List. Block_name must be provided either as a string or as a list."
            dbsExceptionHandler('dbsException-invalid-input', msg)
	cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
        result = []
        for i in cursors:
            d = self.formatCursor(i)
            if d:
                result += d
        return result
