#!/usr/bin/env python
"""
This module provides File.List data access object.
"""
__revision__ = "$Id: List.py,v 1.9 2009/11/30 09:53:44 akhukhun Exp $"
__version__ = "$Revision: 1.9 $"

from WMCore.Database.DBFormatter import DBFormatter

class List(DBFormatter):
    """
    File List DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner
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

        self.formatkeys = {"DATASET_DO":["DATASET_ID", "DATASET"],
                           "BLOCK_DO":["BLOCK_ID", "BLOCK_NAME"],
                           "FILE_TYPE_DO":["FILE_TYPE_ID", "FILE_TYPE"]}
        
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


    def execute(self, dataset = "", block = "", lfn = "",  \
                conn = None, transaction = False):
        """
        dataset: /a/b/c
        block: /a/b/c#d
        lfn: /store/ .....root
        """	
        sql = self.sql
        binds = {}
            
        if not block == "":
            sql += "WHERE B.BLOCK_NAME = :block"
            binds.update({"block":block})
            if not lfn == "":
                sql += " AND F.LOGICAL_FILE_NAME %s :lfn" % ("=","like")["%" in lfn]
                binds.update({"lfn":lfn})
        elif not dataset == "": 
            sql += "WHERE D.DATASET = :dataset"
            binds.update({"dataset":dataset})
            if not lfn == "":
                sql += " AND F.LOGICAL_FILE_NAME %s :lfn" % ("=","like")["%" in lfn]
                binds.update({"lfn":lfn})
        elif not lfn == "":
            sql += "WHERE F.LOGICAL_FILE_NAME = :lfn" 
            binds.update({"lfn":lfn})
        else:
            raise Exception("Either dataset or block must be provided")
        
        result = self.dbi.processData(sql, binds, conn, transaction)
        return self.formatDict(result)
