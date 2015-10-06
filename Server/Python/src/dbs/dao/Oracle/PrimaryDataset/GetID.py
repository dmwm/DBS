#!/usr/bin/env python
"""
This module provides PrimaryDataset.GetID data access object.
"""
from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class GetID(DBFormatter):
    """
    PrimaryDataset GetID DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
"""
SELECT P.PRIMARY_DS_ID, P.PRIMARY_DS_NAME
FROM %sPRIMARY_DATASETS P 
""" % (self.owner)

    def execute(self, conn, name, transaction = False):
        """
        returns id for a given primary dataset name
        """
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed", "Oracle/PrimaryDataset/GetID. Expects db connection from upper layer.")

        sql = self.sql
        sql += "WHERE P.PRIMARY_DS_NAME = :primarydataset" 
        binds = {"primarydataset":name}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
	if len(plist) < 1: return -1
        return plist[0]["primary_ds_id"]
