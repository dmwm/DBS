#!/usr/bin/env python
"""
This module provides FileBuffer.FindDuplicates data access object.
"""
__revision__ = "$Id: FindDuplicates.py,v 1.1 2010/05/27 19:37:25 afaq Exp $"
__version__ = "$Revision: 1.1 $"

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
        self.sql = """SELECT FLBUF.LFN FROM %sFILE_BUFFER FLBUF JOIN %sFILES FL ON FL.LOGICAL_FILE_NAME=FLBUF.LFN""" % (2*(self.owner,))

    def execute(self, conn, transaction=False):

        """
	simple execute
        """	
	binds={}
        if not conn:
            raise Exception("dbs/dao/Oracle/FileBuffer/DeleteFiles expects db connection from up layer.")
	print self.sql
        cursors=self.dbi.processData(self.sql, binds, conn, transaction, returnCursor=True)
        result = self.formatCursor(cursors[0])
        return result

