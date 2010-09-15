#!/usr/bin/env python
"""
This module provides DatasetTYpe.GetID data access object.
"""
__revision__ = "$Id: GetID.py,v 1.1 2009/10/30 16:48:18 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class GetID(DBFormatter):
    """
    DatasetType GetID DAO class.
    """
    def __init__(self, logger, dbi):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % self.dbi.engine.url.username
        self.sql = \
"""
SELECT TP.DATASET_TYPE_ID, TP.DATASET_TYPE
FROM %sDATASET_TYPES TP 
""" % (self.owner)

    def execute(self, name, conn = None, transaction = False):
        """
        returns id for a given dataset type name
        """
        sql = self.sql
        sql += "WHERE TP.DATASET_TYPE = :datasettype" 
        binds = {"datasettype":name}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
        assert len(plist) == 1, \
            "Dataset Type %s does not exist" % name
        return plist[0]["dataset_type_id"]
