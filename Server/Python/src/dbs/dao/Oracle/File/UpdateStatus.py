#!/usr/bin/env python
"""
This module provides File.UpdateStatus data access object.
"""
__revision__ = "$Id: UpdateStatus.py,v 1.2 2010/03/04 17:25:31 afaq Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class UpdateStatus(DBFormatter):

    """
    File Update Statuss DAO class.
    """

    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
	self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = """UPDATE %sFILES SET IS_FILE_VALID = :is_file_valid where LOGICAL_FILE_NAME = :logical_file_name""" %  self.owner 
        
    def execute ( self, conn, transaction, logical_file_name, is_file_valid ):
        """
        for a given file
        """	
	binds = { "logical_file_name" : logical_file_name , "is_file_valid" : is_file_valid }
        result = self.dbi.processData(self.sql, binds, conn, transaction)
    
