#!/usr/bin/env python
"""
This module provides File.UpdateStatus data access object.
"""
__revision__ = "$Id: UpdateStatus.py,v 1.1 2010/03/03 22:35:54 afaq Exp $"
__version__ = "$Revision: 1.1 $"

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
        
    def execute(self, logical_file_name, is_file_valid, conn = None, transaction = False):
        """
        for a given file
        """	
	binds = { "logical_file_name" : logical_file_name , "is_file_valid" : is_file_valid }
        result = self.dbi.processData(self.sql, blockStats, conn, transaction)
