#!/usr/bin/env python
""" DAO Object for DatasetOutputMod_configs table """ 

__revision__ = "$Revision: 1.8 $"
__version__  = "$Id: Insert.py,v 1.8 2010/08/25 21:41:51 afaq Exp $ "

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
            result = self.dbi.processData(self.sql, dataset_output_mod_configsObj, conn, transaction)

