#!/usr/bin/env python
"""
This module provides Block.ListChild data access object.
"""
__revision__ = "$Id: ListChild.py,v 1.2 2010/06/23 21:21:19 afaq Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.Database.MySQLCore import  MySQLInterface

class ListChild(DBFormatter):
    """
    Block List DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
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
	    raise Exception("dbs/dao/Oarcle/BlockParent/ListChild expects db connection from upper layer.")
        sql = self.sql
        binds = {}
	if block_name:
	    binds.update(block_name = block_name)
        else: 
            raise Exception("block_name must be provided")
	cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
        result = self.formatCursor(cursors[0])
        return result
