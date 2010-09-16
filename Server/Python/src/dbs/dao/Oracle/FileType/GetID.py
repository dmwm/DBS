#!/usr/bin/env python
"""
This module provides FileType.GetID data access object.
Light dao object to get the id for a given FileType
"""
__revision__ = "$Id: GetID.py,v 1.2 2009/11/24 10:58:17 akhukhun Exp $"
__version__ = "$Revision: 1.2 $"

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
        self.owner = "%s." % owner
        self.sql = \
"""
SELECT FT.FILE_TYPE_ID, FT.FILE_TYPE
FROM %sFILE_TYPES FT 
""" %  self.owner 
        
    def execute(self, name, conn = None, transaction = False):
        """
        returns id for a given file type
        """	
        sql = self.sql
        sql += "WHERE FT.FILE_TYPE = :filetype"
        binds = {"filetype":name}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
        assert len(plist) == 1, "FileType %s does not exist" % name
        return plist[0]["file_type_id"]
