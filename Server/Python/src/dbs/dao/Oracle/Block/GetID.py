#!/usr/bin/env python
"""
This module provides Block.GetID data access object.
Light dao object to get the id for a give /primds/procds/tier#block
"""
__revision__ = "$Id: GetID.py,v 1.6 2010/08/02 20:41:15 afaq Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.Database.DBFormatter import DBFormatter
class GetID(DBFormatter):
    """
    FileType GetID DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = \
"""
SELECT B.BLOCK_ID, B.BLOCK_NAME
FROM %sBLOCKS B 
""" %  self.owner 
        
    def execute(self, conn, name, transaction = False):
        """
        returns id for a given block = /primds/procds/tier#block
        """	
        sql = self.sql
        sql += "WHERE B.BLOCK_NAME = :block"
        binds = {"block":name}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
	if len(plist) < 1 : return -1
        return plist[0]["block_id"]
