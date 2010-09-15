#!/usr/bin/env python
"""
This module provides PrimaryDataset.List data access object.
"""
__revision__ = "$Id: List.py,v 1.5 2009/11/30 09:53:44 akhukhun Exp $"
__version__ = "$Revision: 1.5 $"


from WMCore.Database.DBFormatter import DBFormatter

class List(DBFormatter):
    """
    PrimaryDataset List DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner
        self.sql = \
"""
SELECT P.PRIMARY_DS_ID, P.PRIMARY_DS_NAME, P.PRIMARY_DS_TYPE_ID,
    PT.PRIMARY_DS_TYPE, P.CREATION_DATE, P.CREATE_BY
FROM %sPRIMARY_DATASETS P
JOIN %sPRIMARY_DS_TYPES PT
ON PT.PRIMARY_DS_TYPE_ID=P.PRIMARY_DS_TYPE_ID
""" % (self.owner, self.owner)

        self.formatkeys = {"PRIMARY_DS_TYPE_DO":["PRIMARY_DS_TYPE_ID", "PRIMARY_DS_TYPE"]}
        
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
        
    def execute(self, pattern = "", conn = None, transaction = False):
        """
        Lists all primary datasets if pattern is not provided.
        """
        sql = self.sql
        if pattern == "":
            result = self.dbi.processData(sql, conn=conn, transaction=transaction)
        else:
            sql += "WHERE P.PRIMARY_DS_NAME %s :primarydsname" % ("=", "like")["%" in pattern]
            binds = {"primarydsname":pattern}
            result = self.dbi.processData(sql, binds, conn, transaction)
        return self.formatDict(result)
