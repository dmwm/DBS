#!/usr/bin/env python
""" DAO Object for DatasetTypes table """ 

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""

            self.sql = """INSERT INTO %sDATASET_ACCESS_TYPES ( DATASET_ACCESS_TYPE_ID, DATASET_ACCESS_TYPE) VALUES
            (:dataset_access_type_id, :dataset_access_type)""" % (self.owner)


    def execute( self, conn, dataset_typesObj, transaction=False ):
	if not conn:
            dbsExceptionHandler("dbsException-db-conn-failed","dbs/dao/Oracle/DatasetType/Insert expects db connection from upper layer.")
	result = self.dbi.processData(self.sql, dataset_typesObj, conn, transaction)
	return


