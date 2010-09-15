# DAO Object for FileOutputMod_config table
# $Revision: 1.1 $
# $Id: Insert.py,v 1.1 2009/10/12 16:48:28 afaq Exp $

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    sql = """INSERT INTO FILE_OUTPUT_MOD_CONFIGS(FILE_OUTPUT_CONFIG_ID, FILE_ID, OUTPUT_MOD_CONFIG_ID) VALUES (:fileoutputconfigid, :fileid, :outputmodconfigid);"""

    def getBinds( self, file_output_mod_configsObj ):
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


    def execute( self, file_output_mod_configsObj ):
            binds = self.getBinds(file_output_mod_configsObj )
            result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
            return