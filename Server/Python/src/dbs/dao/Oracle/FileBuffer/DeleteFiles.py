#!/usr/bin/env python
"""
This module provides FileBuffer.DeleteFiles data access object.
"""
__revision__ = "$Id: DeleteFiles.py,v 1.3 2010/07/09 14:41:00 afaq Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter


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
            raise Exception("dbs/dao/Oracle/FileBuffer/DeleteFiles expects db connection from upper layer.")
        self.dbi.processData(self.sql, logical_file_name, conn, transaction, returnCursor=True)

