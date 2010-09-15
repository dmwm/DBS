#!/usr/bin/env python
"""
This module provides ApplicationExecutable.GetID data access object.
"""
__revision__ = "$Id: List.py,v 1.8 2010/01/25 22:16:28 afaq Exp $"
__version__ = "$Revision: 1.8 $"

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
	SELECT O.OUTPUT_MOD_CONFIG_ID
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
		sql += " P.HASH=:pset_hash"
		binds["pset_hash"]=pset_hash
		setAnd=True
	if not output_label == "":
                if setAnd : sql += " AND "
		else : sql += " WHERE "
	        sql += " O.OUTPUT_MODULE_LABEL=:output_module_label"
	        binds["output_module_label"]=output_label
	#if app == release_version == pset_hash  == "":
	#    raise Exception("Either app_name, release_version or pset_hash must be provided")	

	if not conn:
	    conn = self.dbi.connection()
		
        cursor = conn.connection.cursor()
        cursor.execute(sql, binds)
        result = self.formatCursor(cursor)
        conn.close()
        return result
	    
