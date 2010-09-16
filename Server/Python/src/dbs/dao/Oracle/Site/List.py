#!/usr/bin/env python
"""
This module provides PrimaryDSType.List data access object.
"""
__revision__ = "$Id: List.py,v 1.4 2010/03/05 20:12:34 yuyi Exp $"
__version__ = "$Revision: 1.4 $"


from WMCore.Database.DBFormatter import DBFormatter
class List(DBFormatter):
    """
    PrimaryDSType List DAO class.
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

    def execute(self, conn, pattern = "", transaction = False):
        """
        Lists all sites types if pattern is not provided.
        """
	if not conn:
	    raise Exception("dbs/dao/Oracle/Site/List expects db connection from up layer.")
        sql = self.sql
        if pattern == "":
            result = self.dbi.processData(sql, conn=conn, transaction=transaction)
        else:
            sql += "WHERE S.SITE_NAME = :site_name" 
            binds = {"site_name":pattern}
            result = self.dbi.processData(sql, binds, conn, transaction)
        return self.formatDict(result)
