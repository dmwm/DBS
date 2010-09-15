#!/usr/bin/env python
"""
This module provides Site.GetID data access object.
"""
__revision__ = "$Id: GetID.py,v 1.2 2009/10/30 16:52:49 akhukhun Exp $"
__version__ = "$Revision: 1.2 $"


from WMCore.Database.DBFormatter import DBFormatter
class GetID(DBFormatter):
    """
    Site GetID DAO class.
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

    def execute(self, name, conn = None, transaction = False):
        """
        returns id for a give site
        """
        sql = self.sql
        sql += "WHERE S.SITE_NAME = :sitename" 
        binds = {"sitename":name}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
        assert len(plist) == 1, \
            "Site %s does not exist." % name
        return plist[0]["site_id"]
