#!/usr/bin/env python
"""
This module provides Dataset.GetID data access object.
Light dao object to get the id for a give /primds/procds/tier
"""
__revision__ = "$Id: GetID.py,v 1.5 2009/12/22 21:52:26 afaq Exp $"
__version__ = "$Revision: 1.5 $"

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
        self.owner = "%s." % owner
        self.sql = \
"""
SELECT D.DATASET_ID
FROM %sDATASETS D 
""" % ( self.owner )
        
	
    def execute(self, name, conn = None, transaction = False):
        """
        returns id for a given dataset = /primds/procds/tier
        """	
        sql = self.sql
        sql += "WHERE D.DATASET = :dataset"
        binds = {"dataset":name}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
        assert len(plist) == 1, "Dataset %s does not exist" % name
        return plist[0]["dataset_id"]
