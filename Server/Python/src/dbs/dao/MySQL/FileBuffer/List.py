#!/usr/bin/env python
"""
This module provides File.List data access object.
"""
__revision__ = "$Id: List.py,v 1.1 2010/05/25 21:00:37 afaq Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter


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
	#all listFile APIs should return the same data structure defined by self.sql
        self.sql = """select FILE_BLOB from %sFILE_BUFFER WHERE BLOCK_ID=:block_id LIMIT 10""" % self.owner

    def execute(self, conn, block_id="", transaction=False):

        """
	simple execute
        """	
        if not conn:
            raise Exception("dbs/dao/Oracle/FileBuffer/List expects db connection from up layer.")
        binds = { "block_id" : block_id }
	
        cursors = self.dbi.processData(self.sql, binds, conn, transaction, returnCursor=True)
        result = self.formatCursor(cursors[0])
        return result

