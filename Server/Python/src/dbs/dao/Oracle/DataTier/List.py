#!/usr/bin/env python
"""
This module provides DataTier.List data access object.
"""
__revision__ = "$Id: List.py,v 1.1 2010/04/16 21:14:03 afaq Exp $"
__version__ = "$Revision: 1.1 $"

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
SELECT DT.DATA_TIER_ID, DT.DATA_TIER_NAME
FROM %sDATA_TIERS DT 
""" % (self.owner)

    def execute(self, conn, dataTier, transaction = False):
        """
        returns id for a given datatier name
        """
	if not conn:
	    raise Exception("dbs/dao/Oracle/DataTier/List expects db connection from up layer.")
        sql = self.sql
	binds={}
	if dataTier:
	    sql += "WHERE DT.DATA_TIER_NAME = :datatier" 
	    binds = {"datatier":dataTier}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
        return plist
