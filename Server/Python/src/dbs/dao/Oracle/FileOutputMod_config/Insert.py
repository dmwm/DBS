#!/usr/bin/env python
""" DAO Object for FileOutputMod_configs table """ 

__revision__ = "$Revision: 1.2 $"
__version__  = "$Id: Insert.py,v 1.2 2009/10/20 02:19:20 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
            self.owner = "%s." % self.dbi.engine.url.username

            self.sql = """INSERT INTO %sFILE_OUTPUT_MOD_CONFIGS ( FILE_OUTPUT_CONFIG_ID, FILE_ID, OUTPUT_MOD_CONFIG_ID) VALUES (:fileoutputconfigid, :fileid, :outputmodconfigid) % (self.owner) ;"""

    def getBinds_delme( self, file_output_mod_configsObj ):
            binds = {}
            if type(file_output_mod_configsObj) == type ('object'):
            	binds = {
			'fileoutputconfigid' : file_output_mod_configsObj['fileoutputconfigid'],
			'fileid' : file_output_mod_configsObj['fileid'],
			'outputmodconfigid' : file_output_mod_configsObj['outputmodconfigid'],
                 }

            elif type(file_output_mod_configsObj) == type([]):
               binds = []
               for item in file_output_mod_configsObj:
                   binds.append({
 	                'fileoutputconfigid' : item['fileoutputconfigid'],
 	                'fileid' : item['fileid'],
 	                'outputmodconfigid' : item['outputmodconfigid'],
 	                })
               return binds


    def execute( self, file_output_mod_configsObj, conn=None, transaction=False ):
            ##binds = self.getBinds( file_output_mod_configsObj )
            result = self.dbi.processData(self.sql, binds, conn, transaction)
            return


