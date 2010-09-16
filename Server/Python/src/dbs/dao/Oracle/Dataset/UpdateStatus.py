#!/usr/bin/env python
"""
This module provides Dataset.UpdateStatus data access object.
"""
__revision__ = "$Id: UpdateStatus.py,v 1.1 2010/03/04 17:59:18 afaq Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class UpdateStatus(DBFormatter):

    """
    Dataset Update Statuss DAO class.
    """

    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
	self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = """UPDATE %sDATASETS SET IS_DATASET_VALID = :is_dataset_valid where DATASET = :dataset""" %  self.owner 
        
    def execute ( self, conn, transaction, dataset, is_dataset_valid ):
        """
        for a given file
        """	
	binds = { "dataset" : dataset , "is_dataset_valid" : is_file_valid }
        result = self.dbi.processData(self.sql, binds, conn, transaction)
    
