#!/usr/bin/env python
"""
This module provides Dataset.GetID data access object.
Light dao object to get the id for a give /primds/procds/tier
"""
__revision__ = "$Id: GetID.py,v 1.4 2009/11/24 10:58:12 akhukhun Exp $"
__version__ = "$Revision: 1.4 $"

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
        returns id for a given path = /primds/procds/tier
        """	
        sql = self.sql
        sql += "WHERE D.DATASET = :path"
        binds = {"path":name}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
        assert len(plist) == 1, "Dataset %s does not exist" % name
        return plist[0]["dataset_id"]
