#!/usr/bin/env python
""" DAO Object for ProcessedDatasets table """ 

__revision__ = "$Revision: 1.3 $"
__version__  = "$Id: Insert.py,v 1.3 2009/11/03 16:41:26 akhukhun Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):
    """ProcessedDataset Insert DAO class"""

    def __init__(self, logger, dbi):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % self.dbi.engine.url.username

        self.sql = \
"""INSERT INTO %sPROCESSED_DATASETS 
(PROCESSED_DS_ID, PROCESSED_DS_NAME) 
VALUES (:processeddsid, :processeddsname)
""" % self.owner

    def execute( self, daoinput, conn=None, transaction=False ):
        """
        daoinput must be validated to have the following keys:
        processeddsid, processeddsname"""
        
        self.dbi.processData(self.sql, daoinput, conn, transaction)
