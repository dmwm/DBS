# DAO Object for OutputModuleConfig table
# $Revision: 1.1 $
# $Id: Insert.py,v 1.1 2009/10/12 16:48:29 afaq Exp $

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    sql = """INSERT INTO OUTPUT_MODULE_CONFIGS(OUTPUT_MOD_CONFIG_ID, APP_EXEC_ID, RELEASE_VERSION_ID, PARAMETER_SET_HASH_ID, OUTPUT_MODULE_LABEL, CREATION_DATE, CREATE_BY) VALUES (:outputmodconfigid, :appexecid, :releaseversionid, :parametersethashid, :outputmodulelabel, :creationdate, :createby);"""

    def getBinds( self, output_module_configsObj ):
            binds = {}
            if type(output_module_configsObj) == type ('object'):
            	binds = {
			'outputmodconfigid' : output_module_configsObj['outputmodconfigid'],
			'appexecid' : output_module_configsObj['appexecid'],
			'releaseversionid' : output_module_configsObj['releaseversionid'],
			'parametersethashid' : output_module_configsObj['parametersethashid'],
			'outputmodulelabel' : output_module_configsObj['outputmodulelabel'],
			'creationdate' : output_module_configsObj['creationdate'],
			'createby' : output_module_configsObj['createby'],
                 }

            elif type(output_module_configsObj) == type([]):
               binds = []
               for item in output_module_configsObj:
                   binds.append({
 	                'outputmodconfigid' : item['outputmodconfigid'],
 	                'appexecid' : item['appexecid'],
 	                'releaseversionid' : item['releaseversionid'],
 	                'parametersethashid' : item['parametersethashid'],
 	                'outputmodulelabel' : item['outputmodulelabel'],
 	                'creationdate' : item['creationdate'],
 	                'createby' : item['createby'],
 	                })
               return binds


    def execute( self, output_module_configsObj ):
            binds = self.getBinds(output_module_configsObj )
            result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
            return