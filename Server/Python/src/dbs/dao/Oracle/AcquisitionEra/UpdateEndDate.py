#!/usr/bin/env python
"""
This module provides acquisitionEra.UpdateEndDate data access object.
"""

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class UpdateEndDate(DBFormatter):
    """
    Acquisition UpdateEndDate DAO class.
    """
    def __init__(self, logger, dbi, owner):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
	self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.sql = """UPDATE %sACQUISITION_ERAS SET END_DATE=:end_date where acquisition_era_name=:acquisition_era_name""" %  self.owner 
        self.logger = logger

    def execute(self, conn, acquisition_era_name,end_date, transaction = False):
        """
        for a given block_id
        """	
	if not conn:
	    dbsExceptionHandler("dbsException-failed-connect2host", "dbs/dao/Oracle/AcquisitionEra/updateEndDate expects db connection from upper layer.", self.logger.exception)
        binds = { "acquisition_era_name" :acquisition_era_name  , "end_date" : end_date  }
        result = self.dbi.processData(self.sql, binds, conn, transaction)
