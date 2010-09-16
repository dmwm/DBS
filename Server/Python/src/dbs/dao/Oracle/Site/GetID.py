#!/usr/bin/env python
"""
This module provides Site.GetID data access object.
"""
__revision__ = "$Id: GetID.py,v 1.6 2010/03/05 20:12:34 yuyi Exp $"
__version__ = "$Revision: 1.6 $"


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
	if not conn:
	    raise Exception("dbs/dao/Oracle/Site/GetID expects db connection from up layer.")
        sql = self.sql
        sql += "WHERE S.SITE_NAME = :site_name" 
        binds = {"site_name":name}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
        assert len(plist) == 1, \
            "Site %s does not exist." % name
        return plist[0]["site_id"]
