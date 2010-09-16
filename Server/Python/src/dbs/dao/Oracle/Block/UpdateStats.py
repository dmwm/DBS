#!/usr/bin/env python
"""
This module provides Block.UpdateStats data access object.
"""
__revision__ = "$Id: UpdateStats.py,v 1.6 2010/03/03 22:35:54 afaq Exp $"
__version__ = "$Revision: 1.6 $"

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
        
    def execute(self, blockStats, conn = None, transaction = False):
        """
        for a given block_id
        """	
        result = self.dbi.processData(self.sql, blockStats, conn, transaction)
