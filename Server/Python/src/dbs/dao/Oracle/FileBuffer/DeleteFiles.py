#!/usr/bin/env python
"""
This module provides FileBuffer.DeleteFiles data access object.
"""
__revision__ = "$Id: DeleteFiles.py,v 1.1 2010/05/25 21:01:55 afaq Exp $"
__version__ = "$Revision: 1.1 $"

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
        self.sql = """DELETE FROM %sFILE_BUFFER WHERE LFN=:lfn""" % self.owner

    def execute(self, conn, lfn={}, transaction=False):

        """
	simple execute
        """	
        if not conn:
            raise Exception("dbs/dao/Oracle/FileBuffer/DeleteFiles expects db connection from up layer.")
        self.dbi.processData(self.sql, lfn, conn, transaction, returnCursor=True)

