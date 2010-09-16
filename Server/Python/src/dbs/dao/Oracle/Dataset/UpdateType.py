#!/usr/bin/env python
"""
This module provides Dataset.UpdateType data access object.
"""
__revision__ = "$Id: UpdateType.py,v 1.3 2010/05/05 14:59:51 yuyi Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.Database.DBFormatter import DBFormatter

class UpdateType(DBFormatter):

    """
    Dataset Update Statuss DAO class.
    """

    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
	self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = """UPDATE %sDATASETS SET DATASET_TYPE_ID = ( select DATASET_TYPE_ID from %sDATASET_ACCESS_TYPES where
	DATASET_ACCESS_TYPE=:dataset_type ) where DATASET = :dataset""" %  ((self.owner,)*2) 
        
    def execute ( self, conn, dataset, dataset_type, transaction=False ):
        """
        for a given file
        """	
	if not conn:
	    raise Exception("No database connection")
	binds = { "dataset" : dataset , "dataset_type" : dataset_type }
        result = self.dbi.processData(self.sql, binds, conn, transaction)
    
