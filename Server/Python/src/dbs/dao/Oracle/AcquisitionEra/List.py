#!/usr/bin/env python
"""
This module provides DataTier.List data access object.
"""
from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class List(DBFormatter):
    """
    DataTier List DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
"""
SELECT AE.ACQUISITION_ERA_NAME, AE.START_DATE, AE.END_DATE, AE.CREATION_DATE, AE.CREATE_BY, AE.DESCRIPTION   
FROM %sACQUISITION_ERAS AE 
""" % (self.owner)

    def execute(self, conn, acquisitionEra="", transaction = False):
	if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed", "Oracle/AcquisitionEra/List. Expects db connection from upper layer.")
        sql = self.sql
	binds={}
	if acquisitionEra:
            op = ("=", "like")["%" in acquisitionEra]
	    sql += "WHERE AE.ACQUISITION_ERA_NAME %s :acquisitionEra" %op 
	    binds = {"acquisitionEra":acquisitionEra}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
        return plist
