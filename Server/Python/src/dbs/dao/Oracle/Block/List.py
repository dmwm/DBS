#!/usr/bin/env python
"""
This module provides Block.List data access object.
"""
__revision__ = "$Id: List.py,v 1.9 2009/12/22 14:23:01 akhukhun Exp $"
__version__ = "$Revision: 1.9 $"

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


    def formatCursor(self, cursor):
        """
        Tested only with cx_Oracle cursor. 
        I suspect it will not work with MySQLdb
        cursor must be already executed.
        use fetchmany(size=arraysize=50)
        """
        keys = [d[0].lower() for d in cursor.description]
        result = []
        rapp = result.append
        while True:
            rows = cursor.fetchmany()
            if not rows: 
                cursor.close()
                break
            for r in rows:
                rapp(dict(zip(keys, r)))
        return result    
    

    def execute(self, dataset = "", block = "", conn = None):
        """
        dataset: /a/b/c
        block: /a/b/c#d
        """	
        if not conn:
            conn = self.dbi.connection()
        sql = self.sql
        binds = {}
        if dataset:
            sql += "WHERE DS.DATASET = :dataset"
            binds.update({"dataset":dataset})
        
            if block:
                sql += " AND B.BLOCK_NAME %s :block" % ("=", "like")["%" in block]
                binds.update({"block":block})
                
        elif block:
            sql += "WHERE B.BLOCK_NAME %s :block" % ("=", "like")["%" in block]
            binds.update({"block":block})
        
        else: 
            raise Exception("Either dataset or block must be provided")
        
        cursor = conn.connection.cursor()
        cursor.execute(sql, binds)
        result = self.formatCursor(cursor)
        conn.close()
        return result

