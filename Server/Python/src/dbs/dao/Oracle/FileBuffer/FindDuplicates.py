#!/usr/bin/env python
"""
This module provides FileBuffer.FindDuplicates data access object.
"""
__revision__ = "$Id: FindDuplicates.py,v 1.3 2010/07/09 14:41:00 afaq Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter


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
            raise Exception("dbs/dao/Oracle/FileBuffer/DeleteFiles expects db connection from upper layer.")
	print self.sql
        cursors=self.dbi.processData(self.sql, binds, conn, transaction, returnCursor=True)
        result = self.formatCursor(cursors[0])
        return result

