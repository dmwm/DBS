#!/usr/bin/env python
""" DAO Object for OutputModuleConfigs table """ 

__revision__ = "$Revision: 1.3 $"
__version__  = "$Id: Insert.py,v 1.3 2009/12/21 21:05:39 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
            DBFormatter.__init__(self, logger, dbi)
	    self.owner = "%s." % owner

            self.sql = """INSERT INTO %sOUTPUT_MODULE_CONFIGS ( OUTPUT_MOD_CONFIG_ID, APP_EXEC_ID, RELEASE_VERSION_ID, PARAMETER_SET_HASH_ID, OUTPUT_MODULE_LABEL, CREATION_DATE, CREATE_BY) VALUES (:output_mod_config_id, :app_exec_id, :release_version_id, :parameter_set_hash_id, :output_module_label, :creation_date, :create_by)""" % (self.owner)

    def execute( self, outputModConfigObj, conn=None, transaction=False ):
            ##binds = self.getBinds( output_module_configsObj )
            result = self.dbi.processData(self.sql, outputModConfigObj, conn, transaction)
            return


