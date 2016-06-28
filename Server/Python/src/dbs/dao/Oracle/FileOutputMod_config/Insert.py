#!/usr/bin/env python
""" DAO Object for FileOutputMod_configs table """ 
from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
            DBFormatter.__init__(self, logger, dbi)
            self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
	    self.logger = logger
            self.sql = """INSERT INTO %sFILE_OUTPUT_MOD_CONFIGS ( FILE_OUTPUT_CONFIG_ID, FILE_ID, OUTPUT_MOD_CONFIG_ID) VALUES (%sSEQ_FC.nextval,
            :file_id, :output_mod_config_id)""" % ((self.owner,)*2)

    def execute( self, conn, binds, transaction=False ):
        result = self.dbi.processData(self.sql, binds, conn, transaction)
