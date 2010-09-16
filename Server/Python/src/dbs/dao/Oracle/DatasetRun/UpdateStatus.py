#!/usr/bin/env python
"""
This module provides DatasetRun.UpdateStatus data access object.
"""
__revision__ = "$Id: UpdateStatus.py,v 1.3 2010/03/16 17:01:46 afaq Exp $"
__version__ = "$Revision: 1.3 $"

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
        self.sql = """UPDATE %sDATASET_RUNS SET COMPLETE = :complete where DATASET_ID = (select DATASET_ID from DATASETS where DATASET=:dataset) and RUN_NUMBER = :run_number""" %  self.owner 
        
    def execute ( self, conn, dataset="", run_number=-1, complete=1, transaction=False ):
        """
        for a given (dataset, run) mark it complete
        """	
	binds = { "dataset" : dataset , "run_number" : run_number, "complete" : complete }
        result = self.dbi.processData(self.sql, binds, conn, transaction)
    

