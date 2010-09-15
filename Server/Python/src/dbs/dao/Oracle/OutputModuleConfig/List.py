#!/usr/bin/env python
"""
This module provides ApplicationExecutable.GetID data access object.
"""
__revision__ = "$Id: List.py,v 1.11 2010/01/29 19:59:28 afaq Exp $"
__version__ = "$Revision: 1.11 $"

from WMCore.Database.DBFormatter import DBFormatter
class List(DBFormatter):
    """
    File GetID DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner
        self.sql = \
	"""
	SELECT R.RELEASE_VERSION,
	    P.PSET_HASH,
	    A.APP_NAME,
	    O.OUTPUT_MODULE_LABEL 
	    from %sOUTPUT_MODULE_CONFIGS O 
	        JOIN %sRELEASE_VERSIONS R
	           ON O.RELEASE_VERSION_ID=R.RELEASE_VERSION_ID
	        JOIN %sAPPLICATION_EXECUTABLES A
	           ON O.APP_EXEC_ID=A.APP_EXEC_ID
	        JOIN %sPARAMETER_SET_HASHES P 
	           ON O.PARAMETER_SET_HASH_ID=P.PARAMETER_SET_HASH_ID
	         """ % ( self.owner, self.owner, self.owner, self.owner )
        
    def execute(self, dataset="",  logical_file_name="", app="", release_version="", pset_hash="", output_label ="",  conn = None, transaction = False):
        """
        returns id for a given application
        """	
	sql=self.sql	
        binds = {}
	setAnd=False
	if dataset:
		sql += " JOIN %sDATASET_OUTPUT_MOD_CONFIGS DC ON DC.OUTPUT_MOD_CONFIG_ID=O.OUTPUT_MOD_CONFIG_ID" % self.owner
		sql += " JOIN %sDATASETS DS ON DS.DATASET_ID=DC.DATASET_ID" % self.owner
	if logical_file_name:
		sql += " JOIN %sFILE_OUTPUT_MOD_CONFIGS FC ON FC.OUTPUT_MOD_CONFIG_ID=O.OUTPUT_MOD_CONFIG_ID" % self.owner
		sql += " JOIN %sFILES FS ON FS.FILE_ID=FC.FILE_ID" % self.owner
	if not app == "":
		sql += " WHERE A.APP_NAME=:app_name"
        	binds["app_name"]=app
		setAnd=True
	if not release_version == "":
		if setAnd : sql += " AND "
		else : sql += " WHERE "
		sql += " R.RELEASE_VERSION=:release_version"
		binds["release_version"]=release_version
		setAnd=True
	if not pset_hash == "":
		if setAnd : sql += " AND "
		else : sql += " WHERE "
		sql += " P.PSET_HASH=:pset_hash"
		binds["pset_hash"]=pset_hash
		setAnd=True
	if not output_label == "":
                if setAnd : sql += " AND "
		else : sql += " WHERE "
	        sql += " O.OUTPUT_MODULE_LABEL=:output_module_label"
	        binds["output_module_label"]=output_label
		setAnd=True
	if dataset:
		if setAnd : sql += " AND "
		else : sql += " WHERE "
		sql += "DS.DATASET=:dataset"
		binds["dataset"]=dataset
		setAnd=True
	    
	if logical_file_name:
		if setAnd : sql += " AND "
		else : sql += " WHERE "
		sql += "FS.LOGICAL_FILE_NAME=:logical_file_name"
		binds["logical_file_name"]=logical_file_name
		setAnd=True
	#print sql
	#print binds
	#if app == release_version == pset_hash  == "":
	#    raise Exception("Either app_name, release_version or pset_hash must be provided")	

	if not conn:
	    conn = self.dbi.connection()
		
        cursor = conn.connection.cursor()
        cursor.execute(sql, binds)
        result = self.formatCursor(cursor)
        conn.close()
        return result
	    
