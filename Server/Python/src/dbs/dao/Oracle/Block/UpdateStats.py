#!/usr/bin/env python
"""
This module provides Block.UpdateStats data access object.
"""
__revision__ = "$Id: UpdateStats.py,v 1.3 2010/01/12 17:37:59 afaq Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter
class UpdateStats(DBFormatter):
    """
    FileType GetID DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner
        self.sql = """UPDATE %sBLOCKS SET FILE_COUNT=:file_count, block_size=:block_size where BLOCK_ID=:block_id""" %  self.owner 
        
    def execute(self, blockStats, conn = None, transaction = False):
        """
        for a given block_id
        """	
        result = self.dbi.processData(self.sql, blockStats, conn, transaction)
