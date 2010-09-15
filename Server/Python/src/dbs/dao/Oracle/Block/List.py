#!/usr/bin/env python
"""
This module provides Block.List data access object.
"""
__revision__ = "$Id: List.py,v 1.7 2009/11/30 09:53:45 akhukhun Exp $"
__version__ = "$Revision: 1.7 $"

from WMCore.Database.DBFormatter import DBFormatter

class List(DBFormatter):
    """
    Block List DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner
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

        self.formatkeys = {"DATASET_DO":["DATASET_ID", "DATASET"],
                           "SITE_DO":["ORIGIN_SITE", "SITE_NAME"]}
    
    def formatDict(self, result):
        dictOut = []
        r = result[0]
        descriptions = [str(x) for x in r.keys]
        for i in r.fetchall():
            idict = dict(zip(descriptions, i))     
            for k in self.formatkeys:
                idict[k] = {}
                for kk in self.formatkeys[k]:
                    idict[k][kk] = idict[kk]
                    del idict[kk]
            dictOut.append(idict)     
        return {"result":dictOut} 

    def execute(self, dataset = "", block = "",  \
                conn = None, transaction = False):
        """
        dataset: /a/b/c
        block: /a/b/c#d
        """	
        sql = self.sql
        binds = {}
        if not dataset == "":
            sql += "WHERE DS.DATASET = :dataset"
            binds.update({"dataset":dataset})
        
            if not block == "":
                sql += " AND B.BLOCK_NAME %s :block" % ("=", "like")["%" in block]
                binds.update({"block":block})
                
        elif not block == "":
            sql += "WHERE B.BLOCK_NAME %s :block" % ("=", "like")["%" in block]
            binds.update({"block":block})
        
        else: 
            raise Exception("Either dataset or block must be provided")
        
        result = self.dbi.processData(sql, binds, conn, transaction)
        return self.formatDict(result)

