#!/usr/bin/env python
"""
This module provides Dataset.UpdateType data access object.
"""
from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.dbsUtils import dbsUtils

class UpdateType(DBFormatter):

    """
    Dataset Update Statuss DAO class.
    """

    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = """UPDATE %sDATASETS SET LAST_MODIFIED_BY=:myuser, LAST_MODIFICATION_DATE=:mydate,
        DATASET_ACCESS_TYPE_ID = ( select DATASET_ACCESS_TYPE_ID from %sDATASET_ACCESS_TYPES where
        DATASET_ACCESS_TYPE=:dataset_access_type) where DATASET = :dataset""" %  ((self.owner,)*2)

    def execute ( self, conn, dataset, dataset_access_type, transaction=False ):
        """
        for a given file
        """
        if not conn:
            dbsExceptionHandler("dbsException-db-conn-failed", "Oracle/Dataset/UpdateType.  Expects db connection from upper layer.")
        binds = { "dataset" : dataset , "dataset_access_type" : dataset_access_type ,"myuser": dbsUtils().getCreateBy(), "mydate": dbsUtils().getTime() }
        result = self.dbi.processData(self.sql, binds, conn, transaction)
