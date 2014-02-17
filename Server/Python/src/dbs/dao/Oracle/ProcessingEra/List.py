#!/usr/bin/env python
"""
This module provides DataTier.List data access object.
"""
__vision__ = "$Id: List.py,v 1.3 2010/08/24 18:17:04 yuyi Exp $"
__revision__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class List(DBFormatter):
    """
    Processing version List DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
"""
SELECT PE.PROCESSING_VERSION, PE.CREATION_DATE, PE.CREATE_BY, PE.DESCRIPTION   
FROM %sPROCESSING_ERAS PE 
""" % (self.owner)

    def execute(self, conn, processingV="", transaction = False):
	if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed","Oracle/ProcessingEra/List. Expects db connection from upper layer.")

        sql = self.sql
	binds={}
	if processingV is not None:
            #op = ("=", "like")["%" in  processingV]
            op = "="
	    sql += "WHERE PE.PROCESSING_VERSION %s :processingV" %op 
	    binds = {"processingV":processingV}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
        return plist
