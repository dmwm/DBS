#!/usr/bin/env python
"""
This module provides ApplicationExecutable.GetID data access object.
"""
__revision__ = "$Id: List.py,v 1.5 2010/01/12 17:41:17 afaq Exp $"
__version__ = "$Revision: 1.5 $"

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
	        WHERE """ % ( self.owner, self.owner, self.owner, self.owner )
        
    def execute(self, app="", release_version="", pset_hash="", conn = None, transaction = False):
        """
        returns id for a given application
        """	
	sql=self.sql	
        binds = {}
	setAnd=False
	if not app == "":
		sql += " A.APP_NAME=:app_name"
        	binds["app_name"]=app
		setAnd=True
	if not release_version == "":
		if setAnd : sql += " AND "
		sql += " R.VERSION=:release_version"
		binds["release_version"]=release_version
		setAnd=True
	if not pset_hash == "":
		if setAnd : sql += " AND "
		sql += " P.HASH=:pset_hash"
		binds["pset_hash"]=pset_hash
		setAnd=True

	if app == release_version == pset_hash  == "":
            raise Exception("Either app_name, release_version or pset_hash must be provided")	

        cursor = conn.connection.cursor()
        cursor.execute(sql, binds)
        result = self.formatCursor(cursor)
        conn.close()
        return result
	    

