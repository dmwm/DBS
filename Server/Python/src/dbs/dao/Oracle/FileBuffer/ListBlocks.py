#!/usr/bin/env python
"""
This module provides File.List data access object.
"""
__revision__ = "$Id: ListBlocks.py,v 1.3 2010/07/09 14:41:00 afaq Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter


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
            raise Exception("dbs/dao/Oracle/FileBuffer/List expects db connection from upper layer.")
        binds = {}
        cursors = self.dbi.processData(self.sql, binds, conn, transaction, returnCursor=True)
        result = self.formatCursor(cursors[0])
        return result 

