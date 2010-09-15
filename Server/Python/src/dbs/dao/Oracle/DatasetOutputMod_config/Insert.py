#!/usr/bin/env python
""" DAO Object for DatasetOutputMod_configs table """ 

__revision__ = "$Revision: 1.2 $"
__version__  = "$Id: Insert.py,v 1.2 2009/10/20 02:19:19 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):

    def __init__(self, logger, dbi):
            DBFormatter.__init__(self, logger, dbi)
            self.owner = "%s." % self.dbi.engine.url.username

            self.sql = """INSERT INTO %sDATASET_OUTPUT_MOD_CONFIGS ( DS_OUTPUT_MOD_CONF_ID, DATASET_ID, OUTPUT_MOD_CONFIG_ID) VALUES (:dsoutputmodconfid, :datasetid, :outputmodconfigid) % (self.owner) ;"""

    def getBinds_delme( self, dataset_output_mod_configsObj ):
            binds = {}
            if type(dataset_output_mod_configsObj) == type ('object'):
            	binds = {
			'dsoutputmodconfid' : dataset_output_mod_configsObj['dsoutputmodconfid'],
			'datasetid' : dataset_output_mod_configsObj['datasetid'],
			'outputmodconfigid' : dataset_output_mod_configsObj['outputmodconfigid'],
                 }

            elif type(dataset_output_mod_configsObj) == type([]):
               binds = []
               for item in dataset_output_mod_configsObj:
                   binds.append({
 	                'dsoutputmodconfid' : item['dsoutputmodconfid'],
 	                'datasetid' : item['datasetid'],
 	                'outputmodconfigid' : item['outputmodconfigid'],
 	                })
               return binds


    def execute( self, dataset_output_mod_configsObj, conn=None, transaction=False ):
            ##binds = self.getBinds( dataset_output_mod_configsObj )
            result = self.dbi.processData(self.sql, binds, conn, transaction)
            return


