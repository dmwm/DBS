#!/usr/bin/env python
"""
This module provides File.UpdateStatus data access object.
"""
__revision__ = "$Id: UpdateStatus.py,v 1.4 2010/03/15 16:30:05 afaq Exp $"
__version__ = "$Revision: 1.4 $"

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
        
    def execute ( self, conn, logical_file_name, is_file_valid, transaction=False ):
        """
        for a given file
        """	
	if not conn:
	    raise Exception("dbs/dao/Oracle/File/UpdateStatus expects db connection from up layer.")
	binds = { "logical_file_name" : logical_file_name , "is_file_valid" : is_file_valid }
        result = self.dbi.processData(self.sql, binds, conn, transaction)
    
