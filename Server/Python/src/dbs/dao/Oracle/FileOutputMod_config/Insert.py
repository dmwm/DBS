#!/usr/bin/env python
""" DAO Object for FileOutputMod_configs table """ 

__revision__ = "$Revision: 1.3 $"
__version__  = "$Id: Insert.py,v 1.3 2010/01/05 22:34:32 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
            DBFormatter.__init__(self, logger, dbi)
            self.owner = "%s." %owner
	    self.logger = logger
            self.sql = """INSERT INTO %sFILE_OUTPUT_MOD_CONFIGS ( FILE_OUTPUT_CONFIG_ID, FILE_ID, OUTPUT_MOD_CONFIG_ID) VALUES (:file_output_config_id, :file_id, :output_mod_config_id)""" % (self.owner)

    def execute( self, binds, conn=None, transaction=False ):
            result = self.dbi.processData(self.sql, binds, conn, transaction)

