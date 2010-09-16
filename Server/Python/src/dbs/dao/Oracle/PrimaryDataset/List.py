#!/usr/bin/env python
"""
This module provides PrimaryDataset.List data access object.
"""
__revision__ = "$Id: List.py,v 1.7 2009/12/22 14:23:43 akhukhun Exp $"
__version__ = "$Revision: 1.7 $"


from WMCore.Database.DBFormatter import DBFormatter

class List(DBFormatter):
    """
    PrimaryDataset List DAO class.
    """
    def __init__(self, logger, dbi, owner=""):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = ("", "%s." % owner)[bool(owner)]
        self.sql = \
"""
SELECT P.PRIMARY_DS_ID, P.PRIMARY_DS_NAME, P.PRIMARY_DS_TYPE_ID,
    PT.PRIMARY_DS_TYPE, P.CREATION_DATE, P.CREATE_BY
FROM %sPRIMARY_DATASETS P
JOIN %sPRIMARY_DS_TYPES PT
ON PT.PRIMARY_DS_TYPE_ID=P.PRIMARY_DS_TYPE_ID
""" % (self.owner, self.owner)

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
        
    def execute(self, primarydataset="", conn = None):
        """
        Lists all primary datasets if pattern is not provided.
        """
        if not conn:
            conn = self.dbi.connection()
            
        sql = self.sql
        cursor = conn.connection.cursor()
        
        if not primarydataset:
            cursor.execute(sql)
        else:
            sql += "WHERE P.PRIMARY_DS_NAME %s :primarydsname" % ("=", "like")["%" in primarydataset]
            binds = {"primarydsname":primarydataset}
            cursor.execute(sql, binds)
            
        result = self.formatCursor(cursor)
        conn.close()
        return result
