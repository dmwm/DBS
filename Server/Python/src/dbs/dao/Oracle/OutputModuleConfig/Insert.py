#!/usr/bin/env python
""" DAO Object for OutputModuleConfigs table """ 

__revision__ = "$Revision: 1.2 $"
__version__  = "$Id: Insert.py,v 1.2 2009/10/20 02:19:21 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
            self.owner = "%s." % self.dbi.engine.url.username

            self.sql = """INSERT INTO %sOUTPUT_MODULE_CONFIGS ( OUTPUT_MOD_CONFIG_ID, APP_EXEC_ID, RELEASE_VERSION_ID, PARAMETER_SET_HASH_ID, OUTPUT_MODULE_LABEL, CREATION_DATE, CREATE_BY) VALUES (:outputmodconfigid, :appexecid, :releaseversionid, :parametersethashid, :outputmodulelabel, :creationdate, :createby) % (self.owner) ;"""

    def getBinds_delme( self, output_module_configsObj ):
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


    def execute( self, output_module_configsObj, conn=None, transaction=False ):
            ##binds = self.getBinds( output_module_configsObj )
            result = self.dbi.processData(self.sql, binds, conn, transaction)
            return


