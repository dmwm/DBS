#!/usr/bin/env python
"""
This module provides Dataset.UpdateStatus data access object.
"""
__revision__ = "$Id: UpdateStatus.py,v 1.4 2010/03/15 16:30:05 afaq Exp $"
__version__ = "$Revision: 1.4 $"

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
        
    def execute ( self, conn, dataset, is_dataset_valid, transaction=False ):
        """
        for a given file
        """	
	if not conn:
	    raise Exception("dbs/dao/Oracle/Dataset/ListStatus expects db connection from up layer.")
	binds = { "dataset" : dataset , "is_dataset_valid" : is_dataset_valid}
        result = self.dbi.processData(self.sql, binds, conn, transaction)
    
