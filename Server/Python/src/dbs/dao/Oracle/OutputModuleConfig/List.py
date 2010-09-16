#!/usr/bin/env python
"""
This module provides ApplicationExecutable.GetID data access object.
"""
__revision__ = "$Id: List.py,v 1.18 2010/08/16 18:50:16 yuyi Exp $"
__version__ = "$Revision: 1.18 $"

from WMCore.Database.DBFormatter import DBFormatter
class List(DBFormatter):
    """
    File List DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql1 = \
	"""
	SELECT R.RELEASE_VERSION,
	    P.PSET_HASH,
	    A.APP_NAME,
	    O.OUTPUT_MODULE_LABEL
	"""
	self.sql2 = \
	"""
	    from %sOUTPUT_MODULE_CONFIGS O 
	        JOIN %sRELEASE_VERSIONS R
	           ON O.RELEASE_VERSION_ID=R.RELEASE_VERSION_ID
	        JOIN %sAPPLICATION_EXECUTABLES A
	           ON O.APP_EXEC_ID=A.APP_EXEC_ID
	        JOIN %sPARAMETER_SET_HASHES P 
	           ON O.PARAMETER_SET_HASH_ID=P.PARAMETER_SET_HASH_ID
	         """ % ( self.owner, self.owner, self.owner, self.owner )
        
    def execute(self, conn, dataset="",  logical_file_name="", app="", release_version="", pset_hash="", 
                output_label ="", block_id=0, transaction = False):
        """
        returns id for a given application
        """	
	if not conn:
	    raise Exception("dbs/dao/Oracle/OutputModuleConfig/List expects db connection from upper layer.")
	#sql=self.sql	
        binds = {}
	setAnd=False
	#add search only block id only for migration dump block. 
	if block_id==0:
	    sql = self.sql1 + self.sql2
	    if dataset:
		sql += " JOIN %sDATASET_OUTPUT_MOD_CONFIGS DC ON DC.OUTPUT_MOD_CONFIG_ID=O.OUTPUT_MOD_CONFIG_ID" % self.owner
		sql += " JOIN %sDATASETS DS ON DS.DATASET_ID=DC.DATASET_ID" % self.owner
	    if logical_file_name:
		sql += " JOIN %sFILE_OUTPUT_MOD_CONFIGS FC ON FC.OUTPUT_MOD_CONFIG_ID=O.OUTPUT_MOD_CONFIG_ID" % self.owner
		sql += " JOIN %sFILES FS ON FS.FILE_ID=FC.FILE_ID" % self.owner
	    if not app == "":
		op = ("=", "like")["%" in app]
		sql += " WHERE A.APP_NAME %s :app_name" % op
        	binds["app_name"]=app
		setAnd=True
	    if not release_version == "":
		op = ("=", "like")["%" in release_version]
		if setAnd : sql += " AND " 
		else : sql += " WHERE "
		sql += " R.RELEASE_VERSION %s :release_version" % op
		binds["release_version"]=release_version
		setAnd=True
	    if not pset_hash == "":
		op = ("=", "like")["%" in pset_hash]
		if setAnd : sql += " AND "
		else : sql += " WHERE "
		sql += " P.PSET_HASH %s :pset_hash" % op
		binds["pset_hash"]=pset_hash
		setAnd=True
	    if not output_label == "":
		op = ("=", "like")["%" in output_label]
                if setAnd : sql += " AND "
		else : sql += " WHERE "
	        sql += " O.OUTPUT_MODULE_LABEL %s :output_module_label" % op
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
	else:
	    #select by block id and return config along with LFN
	    sql=  self.sql1 + " , FS.LOGICAL_FILE_NAME LFN " +  self.sql2 \
	          + " JOIN %sFILE_OUTPUT_MOD_CONFIGS FC ON FC.OUTPUT_MOD_CONFIG_ID=O.OUTPUT_MOD_CONFIG_ID" % self.owner \
		  +  " JOIN %sFILES FS ON FS.FILE_ID=FC.FILE_ID" % self.owner \
		  +  "  WHERE FS.BLOCK_ID = :block_id "
	    binds["block_id"]=block_id
	cursors = self.dbi.processData(sql, binds, conn, transaction=False, returnCursor=True)
	assert len(cursors) == 1, "output module config does not exist"
        result = self.formatCursor(cursors[0])
        return result
	    
