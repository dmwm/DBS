#!/usr/bin/env python
"""
This module provides Block.List data access object.
"""
__revision__ = "$Id: List.py,v 1.2 2010/06/23 21:21:18 afaq Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.Database.MySQLCore import  MySQLInterface

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
        self.sql = """SELECT BP.BLOCK_NAME FROM %sBLOCKS BP
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
	    raise Exception("dbs/dao/Oarcle/BlockParent/List expects db connection from upper layer.")
        sql = self.sql
        binds = {}
	if block_name:
	    binds.update(block_name = block_name)
        else: 
            raise Exception("block_name must be provided")
	cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
        result = self.formatCursor(cursors[0])
        return result
