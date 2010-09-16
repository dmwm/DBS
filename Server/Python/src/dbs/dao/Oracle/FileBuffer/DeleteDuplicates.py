#!/usr/bin/env python
"""
This module provides FileBuffer.DeleteDuplicates data access object.
"""
__revision__ = "$Id: DeleteDuplicates.py,v 1.2 2010/06/23 21:21:23 afaq Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter


class DeleteDuplicates(DBFormatter):
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

    def execute(self, conn, lfn, transaction=False):

        """
	simple execute
        """	
        if not conn:
            raise Exception("dbs/dao/Oracle/FileBuffer/DeleteFiles expects db connection from upper layer.")
	print self.sql
        self.dbi.processData(self.sql, lfn, conn, transaction)

