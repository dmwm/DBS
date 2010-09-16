#!/usr/bin/env python
"""
This module provides PrimaryDataset.List data access object.
"""
__revision__ = "$Id: List.py,v 1.8 2009/12/27 13:41:25 akhukhun Exp $"
__version__ = "$Revision: 1.8 $"


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

    def execute(self, primary_ds_name="", conn=None):
        """
        Lists all primary datasets if pattern is not provided.
        """
        if not conn:
            conn = self.dbi.connection()
            
        sql = self.sql
        cursor = conn.connection.cursor()
        
        if not primary_ds_name:
            cursor.execute(sql)
        else:
            op = ("=", "like")["%" in primary_ds_name]
            sql += "WHERE P.PRIMARY_DS_NAME %s :primary_ds_name" % op
            binds = {"primary_ds_name":primary_ds_name}
            cursor.execute(sql, binds)
            
        result = self.formatCursor(cursor)
        conn.close()
        return result
