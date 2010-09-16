#!/usr/bin/env python
"""
This module provides FileParent.List data access object.
"""
__revision__ = "$Id: List.py,v 1.9 2010/08/13 14:04:22 yuyi Exp $"
__version__ = "$Revision: 1.9 $"


from WMCore.Database.DBFormatter import DBFormatter

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
SELECT PF.LOGICAL_FILE_NAME parent_logical_file_name, 
       PF.FILE_ID parent_file_id,
       F.LOGICAL_FILE_NAME
FROM %sFILES PF
JOIN %sFILE_PARENTS FP ON FP.PARENT_FILE_ID = PF.FILE_ID
JOIN %sFILES F ON  F.FILE_ID = FP.THIS_FILE_ID 
""" % ((self.owner,)*3)

    def execute(self, conn, logical_file_name='', block_id=0, transaction=False):
        """
        return {} if condition is not provided.
        """
        if not conn:
            raise Exception("dbs/dao/Oracle/FileParent/List expects db connection from upper layer.")
        sql = self.sql
	if logical_file_name:
	    sql += "WHERE F.LOGICAL_FILE_NAME = :logical_file_name"
	    binds = {"logical_file_name":logical_file_name}
	elif block_id != 0:
	    sql += "WHERE F.BLOCK_ID = :block_id"
	    binds ={'block_id':block_id}
	else:
	    return{}
	cursors = self.dbi.processData(sql, binds, conn, transaction=transaction, returnCursor=True)
	if len(cursors) != 1:
	    raise Exception("File Parents does not exist.")
        result = self.formatCursor(cursors[0])
        return result
