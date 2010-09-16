#!/usr/bin/env python
"""
This module provides FileType.GetID data access object.
Light dao object to get the id for a given FileType
"""
__revision__ = "$Id: GetID.py,v 1.6 2010/06/23 21:21:24 afaq Exp $"
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
SELECT FT.FILE_TYPE_ID, FT.FILE_TYPE
FROM %sFILE_DATA_TYPES FT 
""" %  self.owner 
        
    def execute(self, conn, name, transaction = False):
        """
        returns id for a given file type
        """	
	if not conn:
	    raise Exception("dbs/dao/Oracle/FileType/GetID expects db connection from upper layer.")
        sql = self.sql
        sql += "WHERE FT.FILE_TYPE = :filetype"
        binds = {"filetype":name}
        result = self.dbi.processData(sql, binds, conn, transaction)
        plist = self.formatDict(result)
        assert len(plist) == 1, "FileType %s does not exist" % name
        return plist[0]["file_type_id"]
