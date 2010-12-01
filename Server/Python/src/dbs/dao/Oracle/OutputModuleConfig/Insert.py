#!/usr/bin/env python
""" DAO Object for OutputModuleConfigs table """ 

__revision__ = "$Revision: 1.6 $"
__version__  = "$Id: Insert.py,v 1.6 2010/06/23 21:21:24 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""

            self.sql = """INSERT INTO %sOUTPUT_MODULE_CONFIGS ( OUTPUT_MOD_CONFIG_ID, APP_EXEC_ID, RELEASE_VERSION_ID, PARAMETER_SET_HASH_ID, OUTPUT_MODULE_LABEL, CREATION_DATE, CREATE_BY) VALUES (:output_mod_config_id, :app_exec_id, :release_version_id, :parameter_set_hash_id, :output_module_label, :creation_date, :create_by)""" % (self.owner)

    def execute( self, conn, outputModConfigObj, transaction=False ):
	if not conn:
	    raise Exception("dbs/dao/Oracle/OutputModuleConfig/Insert expects db connection from upper layer.")
	result = self.dbi.processData(self.sql, outputModConfigObj, conn, transaction)
	return


