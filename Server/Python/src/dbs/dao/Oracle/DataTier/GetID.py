#!/usr/bin/env python
"""
This module provides DataTier.GetID data access object.
"""
__revision__ = "$Id: GetID.py,v 1.6 2010/08/02 21:49:48 afaq Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetID(DBFormatter):
    """
    DataTier GetID DAO class.
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

    def execute(self, conn, name, transaction = False):
        """
        returns id for a given datatier name
        """
        sql = self.sql
        sql += "WHERE DT.DATA_TIER_NAME = :datatier" 
        binds = {"datatier":name}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
	if len(plist) < 1: return -1
        return plist[0]["data_tier_id"]
