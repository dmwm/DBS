#!/usr/bin/env python
"""
This module provides Dataset.List data access object.
"""
__revision__ = "$Id: List.py,v 1.15 2009/12/27 13:40:31 akhukhun Exp $"
__version__ = "$Revision: 1.15 $"

from WMCore.Database.DBFormatter import DBFormatter

class List(DBFormatter):
    """
    Dataset List DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = ("", "%s." % owner)[bool(owner)]
        self.sql = \
"""
SELECT D.DATASET_ID, D.DATASET, D.IS_DATASET_VALID, 
        D.XTCROSSSECTION, D.GLOBAL_TAG, 
        D.CREATION_DATE, D.CREATE_BY, 
        D.LAST_MODIFICATION_DATE,
        D.DATASET_TYPE_ID, D.PHYSICS_GROUP_ID,
        DP.DATASET_TYPE, PH.PHYSICS_GROUP_NAME
FROM %sDATASETS D 
LEFT OUTER JOIN %sPHYSICS_GROUPS PH ON PH.PHYSICS_GROUP_ID = D.PHYSICS_GROUP_ID
JOIN %sDATASET_TYPES DP on DP.DATASET_TYPE_ID = D.DATASET_TYPE_ID
""" % ((self.owner,)*3)

    def execute(self, dataset="", conn=None):
        """
        dataset key must be of /a/b/c pattern
        """	
        if not conn:
            conn = self.dbi.connection()
            
        sql = self.sql
        cursor = conn.connection.cursor()
        
        if not dataset:
            cursor.execute(sql)
        else:
            op = ("=", "like")["%" in dataset]
            sql += " WHERE D.DATASET %s :dataset" % op 
            binds = {"dataset":dataset}
            cursor.execute(sql, binds)
            
        result = self.formatCursor(cursor)
        conn.close()
        return result
