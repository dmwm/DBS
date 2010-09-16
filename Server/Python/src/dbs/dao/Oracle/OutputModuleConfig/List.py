#!/usr/bin/env python
"""
This module provides ApplicationExecutable.GetID data access object.
"""
__revision__ = "$Id: List.py,v 1.2 2010/01/07 16:27:41 afaq Exp $"
__version__ = "$Revision: 1.2 $"

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
	FROM %sOUTPUT_MODULE_CONFIGS O, %sRELEASE_VERSIONS R, %sAPPLICATION_EXECUTABLES A, %sPARAMETER_SET_HASHES P
	WHERE """ % ( self.owner, self.owner, self.owner, self.owner )
        
    def execute(self, app="", version="", hash="", conn = None, transaction = False):
        """
        returns id for a given application
        """	
	
        binds = {}
	setAnd=False
	if not app == "":
		self.sql += " A.APP_NAME=:app_name"
        	binds["app_name"]=app
		setAnd=True
	if not version == "":
		if setAnd : self.sql += " AND "
		self.sql += " R.VERSION=:version"
		binds["version"]=version
		setAnd=True
	if not hash == "":
		if setAnd : self.sql += " AND "
		self.sql += " P.HASH=:hash"
		binds["hash"]=hash
		setAnd=True

	if app == version == hash  == "":
            raise Exception("Either app_name, version or hash must be provided")	

        cursor = conn.connection.cursor()
        cursor.execute(sql, binds)
        result = self.formatCursor(cursor)
        conn.close()
        return result
	    

