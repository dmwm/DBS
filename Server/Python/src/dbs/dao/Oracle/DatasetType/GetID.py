#!/usr/bin/env python
"""
This module provides DatasetTYpe.GetID data access object.
"""
__revision__ = "$Id: GetID.py,v 1.8 2010/08/02 21:49:51 afaq Exp $"
__version__ = "$Revision: 1.8 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetID(DBFormatter):
    """
    DatasetType GetID DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
	self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else "" 
        self.sql = \
"""
SELECT TP.DATASET_ACCESS_TYPE_ID, TP.DATASET_ACCESS_TYPE
FROM %sDATASET_ACCESS_TYPES TP 
""" % (self.owner)

    def execute(self, conn, name, transaction = False):
        """
        returns id for a given dataset type name
        """
        sql = self.sql
        sql += "WHERE TP.DATASET_ACCESS_TYPE = :datasettype" 
        binds = {"datasettype":name}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
	if len(plist) < 1: return -1
        return plist[0]["dataset_access_type_id"]
