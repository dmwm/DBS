#!/usr/bin/env python
"""
This module provides Block.UpdateStats data access object.
"""
__revision__ = "$Id: UpdateStats.py,v 1.1 2010/01/04 21:02:19 afaq Exp $"
__version__ = "$Revision: 1.1 $"

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
        self.sql = """UPDATE %sBLOCK SET FILE_COUNT=:file_count, block_size=:block_size where BLOCK_ID=:block_id""" %  self.owner 
        
    def execute(self, blockStats, conn = None, transaction = False):
        """
        for a given block_id
        """	
	print self.sql
        result = self.dbi.processData(sql, blockStats, conn, transaction)
