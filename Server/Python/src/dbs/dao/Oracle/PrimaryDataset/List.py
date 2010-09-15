#!/usr/bin/env python
"""
This module provides PrimaryDataset.List data access object.
"""
__revision__ = "$Id: List.py,v 1.2 2009/10/27 17:24:48 akhukhun Exp $"
__version__ = "$Revision: 1.2 $"


from WMCore.Database.DBFormatter import DBFormatter
class List(DBFormatter):
    """
    PrimaryDataset List DAO class.
    """
    def __init__(self, logger, dbi):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % self.dbi.engine.url.username
        self.sql = \
"""
SELECT P.PRIMARY_DS_ID, P.PRIMARY_DS_NAME, P.PRIMARY_DS_TYPE_ID,
    PT.PRIMARY_DS_TYPE, P.CREATION_DATE, P.CREATE_BY
FROM %sPRIMARY_DATASETS P
JOIN %sPRIMARY_DS_TYPES PT
ON PT.PRIMARY_DS_TYPE_ID=P.PRIMARY_DS_TYPE_ID
""" % (self.owner, self.owner)

    def execute(self, pattern = "", conn = None, transaction = False):
        """
        Lists all primary datasets if pattern is not provided.
        """
        sql = self.sql
        if pattern == "":
            result = self.dbi.processData(sql, conn=conn, transaction=transaction)
        else:
            if  pattern.find("%") == -1:
                sql += "WHERE P.PRIMARY_DS_NAME = :primarydsname"
            else:
                sql += "WHERE P.PRIMARY_DS_NAME like  :primarydsname"
            binds = {"primarydsname":pattern}
            result = self.dbi.processData(sql, binds, conn, transaction)
            
        
        ldict = self.formatDict(result)
        output = []
        for idict in ldict:
            dnested = idict
            primarydstypedo = {"primary_ds_type_id":idict["primary_ds_type_id"], 
                               "primary_ds_type":idict["primary_ds_type"]}
            dnested.update({"primary_ds_type_do":primarydstypedo})
            dnested.pop("primary_ds_type_id")
            dnested.pop("primary_ds_type")
            output.append(dnested)
        return output

