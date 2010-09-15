#!/usr/bin/env python
"""
This module provides Block.GetID data access object.
Light dao object to get the id for a give /primds/procds/tier#block
"""
__revision__ = "$Id: GetID.py,v 1.1 2009/11/03 16:41:27 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter
class GetID(DBFormatter):
    """
    FileType GetID DAO class.
    """
    def __init__(self, logger, dbi):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % self.dbi.engine.url.username
        self.sql = \
"""
SELECT B.BLOCK_ID, B.BLOCK_NAME
FROM %sBLOCKS B 
""" %  self.owner 
        
    def execute(self, name, conn = None, transaction = False):
        """
        returns id for a given block = /primds/procds/tier#block
        """	
        sql = self.sql
        sql += "WHERE B.BLOCK_NAME = :block"
        binds = {"block":name}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
        assert len(plist) == 1, "Block %s does not exist" % name
        return plist[0]["block_id"]
