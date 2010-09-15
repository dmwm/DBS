#!/usr/bin/env python
"""
This module provides File.GetID data access object.
"""
__revision__ = "$Id: GetID.py,v 1.2 2009/11/24 10:58:13 akhukhun Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter
class GetID(DBFormatter):
    """
    File GetID DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner
        self.sql = \
"""
SELECT F.FILE_ID
FROM %sFILES F 
""" % ( self.owner )
        
    def execute(self, name, conn = None, transaction = False):
        """
        returns id for a given lfn
        """	
        sql = self.sql
        sql += "WHERE F.LOGICAL_FILE_NAME = :lfn"
        binds = {"lfn":name}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
        assert len(plist) == 1, "File %s does not exist" % name
        return plist[0]["file_id"]
