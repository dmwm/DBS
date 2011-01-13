#!/usr/bin/env python
"""
This module provides Dataset.UpdateStatus data access object.
"""
__revision__ = "$Id: UpdateStatus.py,v 1.5 2010/06/23 21:21:21 afaq Exp $"
__version__ = "$Revision: 1.5 $"

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
            raise Excpetion("dbsException-1", "%s Oracle/Dataset/UpdateStatus.  Expects db connection from upper layer.\n"\
                    %DBSEXCEPTIONS["dbsException-1"])
	binds = { "dataset" : dataset , "is_dataset_valid" : is_dataset_valid}
        result = self.dbi.processData(self.sql, binds, conn, transaction)
    
