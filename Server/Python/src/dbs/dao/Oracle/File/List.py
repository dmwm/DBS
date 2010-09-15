#!/usr/bin/env python
"""
This module provides File.List data access object.
"""
__revision__ = "$Id: List.py,v 1.14 2009/12/27 13:40:46 akhukhun Exp $"
__version__ = "$Revision: 1.14 $"

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
        self.owner = ("","%s." % owner)[bool(owner)]
        self.sql = \
"""
SELECT F.FILE_ID, F.LOGICAL_FILE_NAME, F.IS_FILE_VALID, 
        F.DATASET_ID, D.DATASET,
        F.BLOCK_ID, B.BLOCK_NAME, 
        F.FILE_TYPE_ID, FT.FILE_TYPE,
        F.CHECK_SUM, F.EVENT_COUNT, F.FILE_SIZE,  
        F.BRANCH_HASH_ID, F.ADLER32, F.MD5, 
        F.AUTO_CROSS_SECTION,
        F.CREATION_DATE, F.CREATE_BY, 
        F.LAST_MODIFICATION_DATE, F.LAST_MODIFIED_BY
FROM %sFILES F 
JOIN %sFILE_TYPES FT ON  FT.FILE_TYPE_ID = F.FILE_TYPE_ID 
JOIN %sDATASETS D ON  D.DATASET_ID = F.DATASET_ID 
JOIN %sBLOCKS B ON B.BLOCK_ID = F.BLOCK_ID
""" % ((self.owner,)*4)


    def execute(self, dataset="", block_name="", logical_file_name="", conn=None):
        """
        dataset: /a/b/c
        block_name: /a/b/c#d
        logical_file_name: string
        """	
        if not conn:
            conn = self.dbi.connection()
        sql = self.sql
        binds = {}
        op = ("=","like")["%" in logical_file_name]
            
        if block_name:
            sql += "WHERE B.BLOCK_NAME = :block_name"
            binds.update({"block_name":block_name})
            if logical_file_name:
                sql += " AND F.LOGICAL_FILE_NAME %s :logical_file_name" % op
                binds.update({"logical_file_name":logical_file_name})
        elif dataset: 
            sql += "WHERE D.DATASET = :dataset"
            binds.update({"dataset":dataset})
            if logical_file_name:
                sql += " AND F.LOGICAL_FILE_NAME %s :logical_file_name" % op
                binds.update({"logical_file_name":logical_file_name})
        elif logical_file_name:
            sql += "WHERE F.LOGICAL_FILE_NAME = :logical_file_name" 
            binds.update({"logical_file_name":logical_file_name})
        else:
            raise Exception("Either dataset or block must be provided")
        
        cursor = conn.connection.cursor()
        cursor.execute(sql, binds)
        result = self.formatCursor(cursor)
        conn.close()
        return result 

