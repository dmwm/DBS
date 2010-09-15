#!/usr/bin/env python
"""
This module provides ApplicationExecutable.GetID data access object.
"""
__revision__ = "$Id: GetID.py,v 1.2 2010/01/07 17:30:42 afaq Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter
class GetID(DBFormatter):
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
        
    def execute(self, app="", release_version="", pset_hash="", conn = None, transaction = False):
        """
        returns id for a given application
        """	
	
        binds = {}
	setAnd=False
	if not app == "":
		self.sql += " A.APP_NAME=:app_name"
        	binds["app_name"]=app
		setAnd=True
	if not release_version == "":
		if setAnd : self.sql += " AND "
		self.sql += " R.VERSION=:release_version"
		binds["release_version"]=release_version
		setAnd=True
	if not pset_hash == "":
		if setAnd : self.sql += " AND "
		self.sql += " P.HASH=:pset_hash"
		binds["pset_hash"]=pset_hash
		setAnd=True

	if app == release_version == pset_hash  == "":
            raise Exception("Either app_name, release_version or pset_hash must be provided")	

        result = self.dbi.processData(self.sql, binds, conn, transaction)
        plist = self.formatDict(result)
        assert len(plist) == 1, "output module does not exist"
        return plist[0]["output_mod_config_id"]

