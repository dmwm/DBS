#!/usr/bin/env python
"""
This module provides ProcessedDataset.GetID data access object.
"""
__revision__ = "$Id: GetID.py,v 1.3 2009/11/24 10:58:16 akhukhun Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetID(DBFormatter):
    """
    ProcessedDataset GetID DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner
        self.sql = \
"""
SELECT PD.PROCESSED_DS_ID, PD.PROCESSED_DS_NAME
FROM %sPROCESSED_DATASETS PD 
""" % (self.owner)

    def execute(self, name, conn = None, transaction = False):
        """
        returns id for a given processed dataset name
        """
        sql = self.sql
        sql += "WHERE PD.PROCESSED_DS_NAME = :processeddsname" 
        binds = {"processeddsname":name}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
        if len(plist) == 1:
            return plist[0]["processed_ds_id"]
        else:
            return -1
