#!/usr/bin/env python
"""
This module provides File.List data access object.
"""
__revision__ = "$Id: List.py,v 1.12 2009/12/08 19:30:45 afaq Exp $"
__version__ = "$Revision: 1.12 $"

from WMCore.Database.DBFormatter import DBFormatter
import cx_Oracle

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
        
        cursor = conn.connection.cursor()
        cursor.execute(sql, binds)
	#print sql
	#print binds
        keys = [d[0] for d in cursor.description]

        result = []
        rapp = result.append
        while True:
            rows  = cursor.fetchmany()
            if not rows: break
            for r in rows:
                rapp(dict(zip(keys, r)))

        cursor.close()
	#print result
	return result
        #return {"result":result}
