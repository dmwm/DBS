#!/usr/bin/env python
"""
This module provides File.List data access object.
"""
__revision__ = "$Id: List.py,v 1.2 2009/11/12 15:19:36 akhukhun Exp $"
__version__ = "$Revision: 1.2 $"

def op(pattern):
    """ returns 'like' if pattern includes '%' and '=' otherwise"""
    if pattern.find("%") == -1:
        return '='
    else:
        return 'like'

from WMCore.Database.DBFormatter import DBFormatter
class List(DBFormatter):
    """
    File List DAO class.
    """
    def __init__(self, logger, dbi):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % self.dbi.engine.url.username
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
                sql += " AND F.LOGICAL_FILE_NAME %s :lfn" % op(lfn)
                binds.update({"lfn":lfn})
        elif not dataset == "": 
            sql += "WHERE D.DATASET = :dataset"
            binds.update({"dataset":dataset})
            if not lfn == "":
                sql += " AND F.LOGICAL_FILE_NAME %s :lfn" % op(lfn)
                binds.update({"lfn":lfn})
        #elif not lfn == "":
        #    sql += "WHERE F.LOGICAL_FILE_NAME %s :lfn" % op(lfn)
        #    binds.update({"lfn":lfn})
        else:
            raise Exception("Either dataset or block must be provided")
        
        result = self.dbi.processData(sql, binds, conn, transaction)
        ldict = self.formatDict(result)
        
        output = [{"size":len(ldict)}]
        for idict in ldict:
            datasetdo = {"dataset_id":idict["dataset_id"],
                         "dataset":idict["dataset"]}
            blockdo = {"block_id":idict["block_id"],
                       "block_name":idict["block_name"]}
            filetypedo = {"file_type_id":idict["file_type_id"],
                          "file_type":idict["file_type"]}
            
            
            idict.update({"dataset_do":datasetdo, 
                          "block_do":blockdo,
                          "file_type_do":filetypedo})
            
            for k in ("dataset_id", "dataset", 
                      "block_id", "block_name", 
                      "file_type_id", "file_type"):
                idict.pop(k)
            output.append(idict)
           
        return output 
