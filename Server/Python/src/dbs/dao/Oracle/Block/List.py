#!/usr/bin/env python
"""
This module provides Block.List data access object.
"""
__revision__ = "$Id: List.py,v 1.10 2009/12/27 13:40:20 akhukhun Exp $"
__version__ = "$Revision: 1.10 $"

from WMCore.Database.DBFormatter import DBFormatter

class List(DBFormatter):
    """
    Block List DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = ("","%s." % owner)[bool(owner)]
        self.sql = \
"""
SELECT B.BLOCK_ID, B.BLOCK_NAME, B.OPEN_FOR_WRITING, 
        B.BLOCK_SIZE, B.FILE_COUNT,
        B.DATASET_ID, DS.DATASET,
        B.ORIGIN_SITE, SI.SITE_NAME
FROM %sBLOCKS B
JOIN %sDATASETS DS ON DS.DATASET_ID = B.DATASET_ID
LEFT OUTER JOIN %sSITES SI ON SI.SITE_ID = B.ORIGIN_SITE
""" % ((self.owner,)*3)

    def execute(self, dataset="", block_name="", conn=None):
        """
        dataset: /a/b/c
        block: /a/b/c#d
        """	
        if not conn:
            conn = self.dbi.connection()
        sql = self.sql
        binds = {}
        op = ("=", "like")["%" in block_name]
        if dataset:
            sql += "WHERE DS.DATASET = :dataset"
            binds.update({"dataset":dataset})
        
            if block_name:
                sql += " AND B.BLOCK_NAME %s :block_name" % op
                binds.update({"block_name":block_name})
                
        elif block_name:
            sql += "WHERE B.BLOCK_NAME %s :block_name" % op
            binds.update({"block_name":block_name})
        
        else: 
            raise Exception("Either dataset or block must be provided")
        
        cursor = conn.connection.cursor()
        cursor.execute(sql, binds)
        result = self.formatCursor(cursor)
        conn.close()
        return result
