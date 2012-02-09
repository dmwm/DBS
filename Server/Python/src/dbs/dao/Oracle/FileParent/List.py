#!/usr/bin/env python
"""
This module provides FileParent.List data access object.
"""
__revision__ = "$Id: List.py,v 1.9 2010/08/13 14:04:22 yuyi Exp $"
__version__ = "$Revision: 1.9 $"

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class List(DBFormatter):
    """
    FileParent List DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
"""
SELECT F.LOGICAL_FILE_NAME this_logical_file_name, PF.LOGICAL_FILE_NAME parent_logical_file_name, 
       PF.FILE_ID parent_file_id
FROM %sFILES PF
JOIN %sFILE_PARENTS FP ON FP.PARENT_FILE_ID = PF.FILE_ID
JOIN %sFILES F ON  F.FILE_ID = FP.THIS_FILE_ID 
""" % ((self.owner,)*3)

    def execute(self, conn, logical_file_name='', block_id=0, block_name='', transaction=False):
        """
        return {} if condition is not provided.
        """
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed","Oracle/FileParent/List. Expects db connection from upper layer.")
        
        sql = self.sql
	if logical_file_name:
	    sql += "WHERE F.LOGICAL_FILE_NAME = :logical_file_name"
	    binds = {"logical_file_name":logical_file_name}
	elif block_id != 0:
	    sql += "WHERE F.BLOCK_ID = :block_id"
	    binds ={'block_id':block_id}
        elif block_name:
            sql += "JOIN %sBLOCKS B on B.BLOCK_ID = F.BLOCK_ID Where B.BLOCK_NAME= :block_name" %self.owner
            binds ={'block_name':block_name}
	else:
	    return{}
        self.logger.debug(sql)
	cursors = self.dbi.processData(sql, binds, conn, transaction=transaction, returnCursor=True)
	result = self.formatCursor(cursors[0])
        return result
