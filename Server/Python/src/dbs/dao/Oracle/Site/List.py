#!/usr/bin/env python
"""
This module provides PrimaryDSType.List data access object.
"""
__revision__ = "$Id: List.py,v 1.1 2009/10/27 17:24:49 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"


from WMCore.Database.DBFormatter import DBFormatter
class List(DBFormatter):
    """
    PrimaryDSType List DAO class.
    """
    def __init__(self, logger, dbi):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % self.dbi.engine.url.username
        self.sql = \
"""
SELECT S.SITE_ID, S.SITE_NAME
FROM %sSITES S 
""" % (self.owner)

    def execute(self, pattern = "", conn = None, transaction = False):
        """
        Lists all sites types if pattern is not provided.
        """
        sql = self.sql
        if pattern == "":
            result = self.dbi.processData(sql, conn=conn, transaction=transaction)
        else:
            sql += "WHERE S.SITE_NAME = :sitename" 
            binds = {"sitename":pattern}
            result = self.dbi.processData(sql, binds, conn, transaction)
        return self.formatDict(result)
