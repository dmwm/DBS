#!/usr/bin/env python
"""
This module provides ApplicationExecutable.GetID data access object.
"""
from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class GetID(DBFormatter):
    """
    File GetID DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.logger = logger
        self.sql = \
	"""
	SELECT O.OUTPUT_MOD_CONFIG_ID
	   from %sOUTPUT_MODULE_CONFIGS O 
		JOIN %sRELEASE_VERSIONS R
		    ON O.RELEASE_VERSION_ID=R.RELEASE_VERSION_ID
		JOIN %sAPPLICATION_EXECUTABLES A
		    ON O.APP_EXEC_ID=A.APP_EXEC_ID
		JOIN %sPARAMETER_SET_HASHES P 
		    ON O.PARAMETER_SET_HASH_ID=P.PARAMETER_SET_HASH_ID
		WHERE """ % ( self.owner, self.owner, self.owner, self.owner )
        
    def execute(self, conn, app="", release_version="", pset_hash="", output_label="", 
                global_tag='', transaction = False):
        """
        returns id for a given application
        """
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed","Oracle/OutputModuleConfig/GetID. Expects db connection from upper layer.")

	sql = self.sql
        binds = {}
	setAnd=False
	if not app == "":
		sql += " A.APP_NAME=:app_name"
        	binds["app_name"]=app
		setAnd=True
	if not release_version == "":
		if setAnd : sql += " AND "
		sql += " R.RELEASE_VERSION=:release_version"
		binds["release_version"]=release_version
		setAnd=True
	if not pset_hash == "":
		if setAnd : sql += " AND "
		sql += " P.PSET_HASH=:pset_hash"
		binds["pset_hash"]=pset_hash
		setAnd=True
	if not output_label == "":
		if setAnd : sql += " AND "
		sql += " O.OUTPUT_MODULE_LABEL=:output_module_label"
		binds["output_module_label"]=output_label
                setAnd=True
        if not global_tag == "":
                if setAnd : sql += " AND "
                sql += " O.GLOBAL_TAG=:global_tag"
                binds["global_tag"]=global_tag
	if app == release_version == pset_hash  == global_tag == "":
            dbsExceptionHandler('dbsException-invalid-input', "%s Either app_name, release_version, pset_hash or global_tag must be provided")	

        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
	if len(plist) < 1: return -1
        return plist[0]["output_mod_config_id"]

