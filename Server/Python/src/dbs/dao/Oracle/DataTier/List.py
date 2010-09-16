#!/usr/bin/env python
"""
This module provides DataTier.List data access object.
"""
__revision__ = "$Id: List.py,v 1.5 2010/08/09 18:43:08 yuyi Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.Database.DBFormatter import DBFormatter

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
SELECT DT.DATA_TIER_ID, DT.DATA_TIER_NAME, DT.CREATION_DATE, DT.CREATE_BY  
FROM %sDATA_TIERS DT 
""" % (self.owner)

    def execute(self, conn, dataTier, transaction = False, cache=None):
        """
        returns id for a given datatier name
        """
	if cache:
		ret=cache.get("DATA_TIERS")
		if not ret==None:
			return ret
	if not conn:
	    raise Exception("dbs/dao/Oracle/DataTier/List expects db connection from upper layer.")
        sql = self.sql
	binds={}
	if dataTier:
	    sql += "WHERE DT.DATA_TIER_NAME = :datatier" 
	    binds = {"datatier":dataTier}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
        return plist
