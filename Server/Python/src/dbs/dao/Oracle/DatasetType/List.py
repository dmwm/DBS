#!/usr/bin/env python
"""
This module provides DatasetType.List data access object.
"""
__revision__ = "$Id: List.py,v 1.5 2010/08/09 18:43:08 yuyi Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class List(DBFormatter):
    """
    DatasetType List DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
"""
SELECT DT.DATASET_ACCESS_TYPE FROM %sDATASET_ACCESS_TYPES DT
""" % (self.owner)

    def execute(self, conn, datasetAccessType='', transaction = False):
	if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed","dbs/dao/Oracle/DatasetAccessType/List expects db connection from upper layer.")
        sql = self.sql
	binds={}
	if datasetAccessType:
            op = ("=", "like")["%" in datasetAccessType]
	    sql += "WHERE DT.DATASET_ACCESS_TYPE %s :dataset_access_type" %op 
	    binds = {"dataset_access_type":datasetAccessType}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
        return plist
