#!/usr/bin/env python
"""
This module provides Dataset.UpdateStatus data access object.
"""
__revision__ = "$Id: UpdateStatus.py,v 1.5 2010/06/23 21:21:21 afaq Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.dbsUtils import dbsUtils

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
        self.sql = """UPDATE %sDATASETS SET LAST_MODIFIED_BY=:myuser, LAST_MODIFICATION_DATE=:mydate, 
        IS_DATASET_VALID = :is_dataset_valid where DATASET = :dataset""" %  self.owner 
        
    def execute ( self, conn, dataset, is_dataset_valid, transaction=False ):
        """
        for a given file
        """	
	if not conn:
            dbsExceptionHandler("dbsException-db-conn-failed", "Oracle/Dataset/UpdateStatus.  Expects db connection from upper layer.")
	binds = { "dataset" : dataset , "is_dataset_valid" : is_dataset_valid, "myuser": dbsUtils().getTime(), "mydate": dbsUtils().getCreateBy()}
        result = self.dbi.processData(self.sql, binds, conn, transaction)
    
