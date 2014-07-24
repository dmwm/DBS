#!/usr/bin/env python
""" DAO Object for ParameterSetHashes table """ 

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""

        self.sql = """INSERT INTO %sPARAMETER_SET_HASHES ( PARAMETER_SET_HASH_ID, PSET_HASH, PSET_NAME) VALUES (:parameter_set_hash_id, :pset_hash, :name)""" % (self.owner)

    def execute( self, conn, psetHashObj, transaction=False ):
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed","Oracle/ParameterSetHashe/Insert. Expects db connection from upper layer.")

        result = self.dbi.processData(self.sql, psetHashObj, conn, transaction)
	
