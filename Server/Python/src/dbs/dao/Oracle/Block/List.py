#!/usr/bin/env python
"""
This module provides Block.List data access object.
"""
__revision__ = "$Id: List.py,v 1.3 2009/11/12 15:19:36 akhukhun Exp $"
__version__ = "$Revision: 1.3 $"

def op(pattern):
    """ returns 'like' if pattern includes '%' and '=' otherwise"""
    if pattern.find("%") == -1:
        return '='
    else:
        return 'like'

from WMCore.Database.DBFormatter import DBFormatter
class List(DBFormatter):
    """
    Block List DAO class.
    """
    def __init__(self, logger, dbi):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % self.dbi.engine.url.username
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
                sql += " AND B.BLOCK_NAME %s :block" % op(block)
                binds.update({"block":block})
                
        elif not block == "":
            sql += "WHERE B.BLOCK_NAME %s :block" % op(block)
            binds.update({"block":block})
        
        else: 
            raise Exception("Either dataset or block must be provided")
        
        result = self.dbi.processData(sql, binds, conn, transaction)
        ldict = self.formatDict(result)
        
        output = []
        
        for idict in ldict:
            datasetdo = {"dataset_id":idict["dataset_id"],
                                "dataset":idict["dataset"]}
            originsitedo = {"site_id":idict["origin_site"],
                                  "site_name":idict["site_name"]}
            
            idict.update({"dataset_do":datasetdo, 
                          "originsitedo":originsitedo})
            
            for k in ("origin_site", "site_name",
                      "dataset_id", "dataset"):
                idict.pop(k)
            output.append(idict)
           
        return output 

