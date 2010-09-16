#!/usr/bin/env python
"""
This module provides Block.UpdateStats data access object.
"""
__revision__ = "$Id: UpdateStats.py,v 1.8 2010/06/23 21:21:18 afaq Exp $"
__version__ = "$Revision: 1.8 $"

from WMCore.Database.DBFormatter import DBFormatter
class UpdateStats(DBFormatter):
    """
    Block UpdateStats DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
	self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = """UPDATE %sBLOCKS SET FILE_COUNT=:file_count, BLOCK_SIZE=:block_size where BLOCK_ID=:block_id""" %  self.owner 
        
    def execute(self, conn, blockStats, transaction = False):
        """
        for a given block_id
        """	
	if not conn:
	    raise Exception("dbs/dao/Oracle/Block/UpdateStatus expects db connection from upper layer.")
        result = self.dbi.processData(self.sql, blockStats, conn, transaction)
