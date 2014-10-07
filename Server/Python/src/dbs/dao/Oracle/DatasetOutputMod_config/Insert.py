#!/usr/bin/env python
""" DAO Object for DatasetOutputMod_configs table """ 

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.logger = logger
        self.sql = """INSERT INTO %sDATASET_OUTPUT_MOD_CONFIGS ( DS_OUTPUT_MOD_CONF_ID, DATASET_ID, OUTPUT_MOD_CONFIG_ID) 
				VALUES (%sSEQ_DC.nextval, :dataset_id, :output_mod_config_id)""" % ((self.owner,)*2)

    def execute( self, conn, dataset_output_mod_configsObj, transaction=False ):
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed","Oracle/DatasetOutputMod_config/Insert. Expects db connection from upper layer.")

        result = self.dbi.processData(self.sql, dataset_output_mod_configsObj, conn, transaction)

