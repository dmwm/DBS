#!/usr/bin/env python
"""
This module provides ReleaseVersion.GetID data access object.
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
        self.sql = \
	"""
	SELECT R.RELEASE_VERSION_ID
	FROM %sRELEASE_VERSIONS R WHERE RELEASE_VERSION = :release_version
	""" % ( self.owner )
        
    def execute(self, conn, release_version, transaction = False):
        """
        returns id for a given application
        """
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed","Oracle/ReleaseVersion/GetID. Expects db connection from upper layer.")

        binds = {"release_version":release_version}
        result = self.dbi.processData(self.sql, binds, conn, transaction)
        plist = self.formatDict(result)
	if len(plist) < 1: return -1
        return plist[0]["release_version_id"]

