#!/usr/bin/env python
"""
This module provides AcquisitionEra.GetID data access object.
"""
__revision__ = "$Id: GetID.py,v 1.1 2009/11/03 16:41:26 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetID(DBFormatter):
    """
    AcquisitionEra GetID DAO class.
    """
    def __init__(self, logger, dbi):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % self.dbi.engine.url.username
        self.sql = \
"""
SELECT AE.ACQUISITION_ERA_ID, AE.ACQUISITION_ERA_NAME
FROM %sACQUISITION_ERAS AE 
""" % (self.owner)

    def execute(self, name, conn = None, transaction = False):
        """
        returns id for a given acquisitionera
        """
        sql = self.sql
        sql += "WHERE AE.ACQUISITION_ERA_NAME = :acquisitionera" 
        binds = {"acquisitionera":name}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
        assert len(plist) == 1, \
            "AcquisitionEra %s does not exist" % name
        return plist[0]["acquisition_era_id"]
