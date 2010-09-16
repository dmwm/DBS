#!/usr/bin/env python
"""
This module provides Site.GetID data access object.
"""
__revision__ = "$Id: GetID.py,v 1.8 2010/08/02 21:50:14 afaq Exp $"
__version__ = "$Revision: 1.8 $"


from WMCore.Database.DBFormatter import DBFormatter
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
        sql = self.sql
        sql += " WHERE S.SITE_NAME = :site_name" 
        binds = {"site_name":name}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
	if len(plist) < 1: return -1
        return plist[0]["site_id"]
