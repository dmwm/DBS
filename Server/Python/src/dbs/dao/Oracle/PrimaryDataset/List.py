#!/usr/bin/env python
"""
This module provides PrimaryDataset.List data access object.
"""
__revision__ = "$Id: List.py,v 1.15 2010/08/02 20:41:16 afaq Exp $"
__version__ = "$Revision: 1.15 $"


from WMCore.Database.DBFormatter import DBFormatter

import threading

class List(DBFormatter):
    """
    PrimaryDataset List DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
"""
SELECT P.PRIMARY_DS_ID, P.PRIMARY_DS_NAME, 
       P.CREATION_DATE, P.CREATE_BY,
       PT.PRIMARY_DS_TYPE  
FROM %sPRIMARY_DATASETS P
JOIN %sPRIMARY_DS_TYPES PT ON PT.PRIMARY_DS_TYPE_ID = P.PRIMARY_DS_TYPE_ID
""" % (self.owner, self.owner)

    def execute(self, conn, primary_ds_name="", transaction=False):
        """
        Lists all primary datasets if pattern is not provided.
        """
	
	if not conn:
	    raise Exception("dbs/dao/Oracle/ParimaryDataset/List expects db connection from upper layer.")	    
        sql = self.sql
        binds = {}
        
        if primary_ds_name:
            op = ("=", "like")["%" in primary_ds_name]
            sql += "WHERE P.PRIMARY_DS_NAME %s :primary_ds_name" % op
            binds.update(primary_ds_name=primary_ds_name)
	cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
	assert len(cursors) == 1, "primary DS does not exist"
		
        result = self.formatCursor(cursors[0])
        return result
