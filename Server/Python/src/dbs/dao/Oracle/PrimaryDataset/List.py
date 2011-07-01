#!/usr/bin/env python
"""
This module provides PrimaryDataset.List data access object.
"""
__revision__ = "$Id: List.py,v 1.15 2010/08/02 20:41:16 afaq Exp $"
__version__ = "$Revision: 1.15 $"


from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsException import dbsException,dbsExceptionCode
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler


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

    def execute(self, conn, primary_ds_name="", primary_ds_type="", transaction=False):
        """
        Lists all primary datasets if pattern is not provided.
        """	
	if not conn:
	    dbsExceptionHandler('dbsException-db-conn-failed', "ParimaryDataset/List expects db connection from upper layer.")	    
        sql = self.sql
        binds = {}
        #import pdb
        #pdb.set_trace()
        if primary_ds_name and primary_ds_type in ('', None, '%'):
            op = ("=", "like")["%" in primary_ds_name]
            sql += "WHERE P.PRIMARY_DS_NAME %s :primary_ds_name" % op
            binds.update(primary_ds_name=primary_ds_name)
        elif primary_ds_type and primary_ds_name in ('', None, '%'):
            op = ("=", "like")["%" in primary_ds_type]
            sql += "WHERE PT.PRIMARY_DS_TYPE %s :primary_ds_type" % op
            binds.update(primary_ds_type=primary_ds_type)
        elif primary_ds_name and primary_ds_type:
            op = ("=", "like")["%" in primary_ds_name]
            op1 = ("=", "like")["%" in primary_ds_type]
            sql += "WHERE P.PRIMARY_DS_NAME %s :primary_ds_name and PT.PRIMARY_DS_TYPE %s :primary_ds_type"\
                %(op,op1)
            binds.update(primary_ds_name=primary_ds_name)
            binds.update(primary_ds_type=primary_ds_type)
        else:
            pass
	cursors = self.dbi.processData(sql, binds, conn, transaction, returnCursor=True)
	if len(cursors) == 0 :
            return []
	else:	
            return self.formatCursor(cursors[0])
