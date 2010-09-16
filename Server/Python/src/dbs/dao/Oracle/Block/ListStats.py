#!/usr/bin/env python
"""
This module provides Block.ListStats data access object.
Block parameters based on current conditions at DBS, are listed by this DAO
"""
__revision__ = "$Id: ListStats.py,v 1.3 2010/02/11 18:03:23 afaq Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter

class ListStats(DBFormatter):
    """
    FileType GetID DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
	self.sql = """SELECT count(*) AS FILE_COUNT, 
			SUM(FILE_SIZE) AS BLOCK_SIZE,
			f.BLOCK_ID AS BLOCK_ID
			FROM %sFILES f 
				WHERE f.IS_FILE_VALID=1
				    AND f.BLOCK_ID = :block_id  group by BLOCK_ID""" % (self.owner)
	
    def execute(self, block_id, conn = None, transaction = False):
        """
        returns id for a given block = /primds/procds/tier#block
        """	
        binds = {"block_id": block_id}
        result = self.dbi.processData(self.sql, binds, conn, transaction)
        plist = self.formatDict(result)
        assert len(plist) == 1, "Block %s does not exist" % name
        return plist[0]

