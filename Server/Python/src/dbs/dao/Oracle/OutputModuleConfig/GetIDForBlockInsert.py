#!/usr/bin/env python
"""
This module provides OutputModuleConfig.GetIDForBlockInsert data access object.
To be used with block Insertion
"""

from WMCore.Database.DBFormatter import DBFormatter

class GetIDForBlockInsert(DBFormatter):
    """
    File GetID DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""

        self.sql = """SELECT O.OUTPUT_MOD_CONFIG_ID from %sOUTPUT_MODULE_CONFIGS O 
                        INNER JOIN %sRELEASE_VERSIONS R ON O.RELEASE_VERSION_ID=R.RELEASE_VERSION_ID
                        INNER JOIN %sAPPLICATION_EXECUTABLES A ON O.APP_EXEC_ID=A.APP_EXEC_ID
                        INNER JOIN %sPARAMETER_SET_HASHES P ON O.PARAMETER_SET_HASH_ID=P.PARAMETER_SET_HASH_ID
                        WHERE A.APP_NAME = :app_name
                         AND R.RELEASE_VERSION=:release_version
                         AND P.PSET_HASH=:pset_hash
                         AND O.OUTPUT_MODULE_LABEL=:output_module_label
                         """ % ( self.owner, self.owner, self.owner, self.owner )
        
    def execute(self, conn, app, release_version, pset_hash, output_label, transaction = False):
        """
        returns id for a given application

        This always requires all four variables to be set, because
        you better have them in blockInsert
        """	
        binds = {}
        binds["app_name"]=app
        binds["release_version"]=release_version
        binds["pset_hash"]=pset_hash
        binds["output_module_label"]=output_label

        result = self.dbi.processData(self.sql, binds, conn, transaction)

        plist = self.formatDict(result)

	if len(plist) < 1: return -1
        return plist[0]["output_mod_config_id"]
