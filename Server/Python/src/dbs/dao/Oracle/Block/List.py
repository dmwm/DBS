#!/usr/bin/env python
"""
This module provides Block.List data access object.
"""
__revision__ = "$Id: List.py,v 1.16 2010/03/05 15:32:53 yuyi Exp $"
__version__ = "$Revision: 1.16 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.Database.MySQLCore import  MySQLInterface

class List(DBFormatter):
    """
    Block List DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
"""
SELECT B.BLOCK_ID, B.BLOCK_NAME, B.OPEN_FOR_WRITING, 
        B.BLOCK_SIZE, B.FILE_COUNT,
        B.DATASET_ID, DS.DATASET,
        SI.SITE_NAME
FROM %sBLOCKS B
JOIN %sDATASETS DS ON DS.DATASET_ID = B.DATASET_ID
LEFT OUTER JOIN %sSITES SI ON SI.SITE_ID = B.ORIGIN_SITE
""" % ((self.owner,)*3)

    def execute(self, conn, dataset="", block_name="", site_name="", transaction = False):
        """
        dataset: /a/b/c
        block: /a/b/c#d
        """	
	if not conn:
	    raise Exception("dbs/dao/Oarcle/Block/List expects db connection from up layer.")
        sql = self.sql
        binds = {}
        op = ("=", "like")["%" in block_name]
        if dataset:
            sql += "WHERE DS.DATASET = :dataset"
            binds.update(dataset=dataset)
        
            if block_name:
                sql += " AND B.BLOCK_NAME %s :block_name" % op
                binds.update({"block_name":block_name})
            if site_name:
                sql += " AND SI.SITE_NAME = :site_name"
                binds.update(site_name = site_name)
                
        elif block_name:
            sql += "WHERE B.BLOCK_NAME = :block_name"
            binds.update(block_name = block_name)
            
            if site_name:
                sql += " AND SI.SITE_NAME = :site_name"
                binds.update(site_name = site_name)
                
        elif site_name:
            sql += " WHERE SI.SITE_NAME = :site_name"
            binds.update( site_name = site_name)
            
        else: 
            raise Exception("dataset, block_name or site_name must be provided")
	cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
	assert len(cursors) == 1, "block does not exist"
#if self.dbi.engine.dialect.name == 'mysql' :
        result = self.formatCursor(cursors[0])
        return result
