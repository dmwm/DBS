#!/usr/bin/env python
"""
This module provides Dataset.List data access object.
"""
__revision__ = "$Id: List.py,v 1.14 2009/12/22 14:23:13 akhukhun Exp $"
__version__ = "$Revision: 1.14 $"

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

    def execute(self, dataset="", conn = None):
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
            sql += " WHERE D.DATASET %s :dataset" % ("=", "like")["%" in dataset]
            binds = {"dataset":dataset}
            cursor.execute(sql, binds)
            
        result = self.formatCursor(cursor)
        conn.close()
        return result
