#!/usr/bin/env python
"""
This module provides Site.GetID data access object.
"""
from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class GetID(DBFormatter):
    """
    Site GetID DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
"""
SELECT S.SITE_ID, S.SITE_NAME
FROM %sSITES S 
""" % (self.owner)

    def execute(self, conn, name="", transaction = False):
        """
        returns id for a give site
        """
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed", "Oracle/Site/GetID. Expects db connection from upper layer.")

        sql = self.sql
        sql += " WHERE S.SITE_NAME = :site_name" 
        binds = {"site_name":name}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
	if len(plist) < 1: return -1
        return plist[0]["site_id"]
