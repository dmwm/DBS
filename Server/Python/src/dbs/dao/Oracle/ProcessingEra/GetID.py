#!/usr/bin/env python
"""
This module provides ProcessingEra.GetID data access object.
"""
__revision__ = "$Id: GetID.py,v 1.1 2009/10/30 16:52:05 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetID(DBFormatter):
    """
    ProcessingEra GetID DAO class.
    """
    def __init__(self, logger, dbi):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % self.dbi.engine.url.username
        self.sql = \
"""
SELECT PE.PROCESSING_ERA_ID, PE.PROCESSING_VERSION
FROM %sPROCESSING_ERAS PE
""" % (self.owner)

    def execute(self, name, conn = None, transaction = False):
        """
        returns id for a given processing version name
        """
        sql = self.sql
        sql += "WHERE PE.PROCESSING_VERSION = :processingversion" 
        binds = {"processingversion":name}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
        assert len(plist) == 1, \
            "ProcessingVersion %s does not exist" % name
        return plist[0]["processing_era_id"]
