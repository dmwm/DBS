#!/usr/bin/env python
"""
This module provides Dataset.GetID data access object.
Light dao object to get the id for a give /primds/procds/tier
"""
__revision__ = "$Id: GetDSConfigs.py,v 1.4 2010/06/23 21:21:21 afaq Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetDSConfigs(DBFormatter):
    """
    Dataset GetID DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
	"""
	SELECT OMC.OUTPUT_MOD_CONFIG_ID 
	    FROM %sOUTPUT_MODULE_CONFIGS OMC
	    JOIN %sRELEASE_VERSIONS RV ON RV.RELEASE_VERSION_ID = OMC.RELEASE_VERSION_ID
	    JOIN %sPARAMETER_SET_HASHES PSH ON PSH.PARAMETER_SET_HASH_ID = OMC.PARAMETER_SET_HASH_ID
	    JOIN %sAPPLICATION_EXECUTABLES AEX ON AEX.APP_EXEC_ID = OMC.APP_EXEC_ID
	    JOIN %sDATASET_OUTPUT_MOD_CONFIGS DOMC ON  OMC.OUTPUT_MOD_CONFIG_ID = DOMC.OUTPUT_MOD_CONFIG_ID
	    JOIN %sDATASETS D ON D.DATASET_ID = DOMC.DATASET_ID WHERE DATASET=:dataset""" % ( 6 * (self.owner,) )
        
    def execute(self, conn, dataset, transaction = False):
        """
        returns id for a given dataset = /primds/procds/tier
        """	
	if not conn:
	    raise Exception("dbs/dao/Oracle/Dataset/GetID expects db connection from upper layer.")
        sql = self.sql
        binds = {"dataset":dataset}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
        return plist
