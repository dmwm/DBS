#!/usr/bin/env python
""" DAO Object for DatasetOutputMod_configs table """ 

__revision__ = "$Revision: 1.6 $"
__version__  = "$Id: Insert.py,v 1.6 2010/03/05 16:32:48 yuyi Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy import exceptions

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
            DBFormatter.__init__(self, logger, dbi)
            self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
	    self.logger = logger
            self.sql = """INSERT INTO %sDATASET_OUTPUT_MOD_CONFIGS ( DS_OUTPUT_MOD_CONF_ID, DATASET_ID, OUTPUT_MOD_CONFIG_ID) 
				VALUES (:ds_output_mod_conf_id, :dataset_id, :output_mod_config_id)""" % (self.owner)

    def execute( self, conn, dataset_output_mod_configsObj, transaction=False ):
	if not conn:
	    raise Exception("dbs/dao/Oracle/DatasetOutputMod_config/Insert expects db connection from up layer.")
	try:
            result = self.dbi.processData(self.sql, dataset_output_mod_configsObj, conn, transaction)
	except Exception, ex:
		if str(ex).lower().find("unique constraint") != -1 or str(ex).lower().find("duplicate") != -1:
			self.logger.warning("Unique constraint violation being ignored...")
			self.logger.warning("%s" % ex)
		else:
			raise	

