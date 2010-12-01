#!/usr/bin/env python
"""
This module provides Dataset.GetID data access object.
Light dao object to get the id for a give /primds/procds/tier
"""
__revision__ = "$Id: GetID.py,v 1.11 2010/08/02 21:49:50 afaq Exp $"
__version__ = "$Revision: 1.11 $"

from WMCore.Database.DBFormatter import DBFormatter
class GetID(DBFormatter):
    """
    Dataset GetID DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
"""
SELECT D.DATASET_ID
FROM %sDATASETS D 
""" % ( self.owner )
        
	
    def execute(self, conn, dataset, transaction = False):
        """
        returns id for a given dataset = /primds/procds/tier
        """	
        sql = self.sql
        sql += "WHERE D.DATASET = :dataset"
        binds = {"dataset":dataset}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
	if len(plist) < 1: return -1
        return plist[0]["dataset_id"]
