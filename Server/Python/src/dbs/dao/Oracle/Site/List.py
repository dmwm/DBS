#!/usr/bin/env python
"""
This module provides PrimaryDSType.List data access object.
"""
__revision__ = "$Id: List.py,v 1.6 2010/06/23 21:21:26 afaq Exp $"
__version__ = "$Revision: 1.6 $"


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

    def execute(self, conn, site_name= "", transaction = False):
        """
        Lists all sites types if site_name is not provided.
        """
	if not conn:
	    raise Exception("dbs/dao/Oracle/Site/List expects db connection from upper layer.")
        sql = self.sql
        if site_name == "":
            result = self.dbi.processData(sql, conn=conn, transaction=transaction)
        else:
            sql += "WHERE S.SITE_NAME = :site_name" 
            binds = { "site_name" : site_name }
            result = self.dbi.processData(sql, binds, conn, transaction)
        return self.formatDict(result)
