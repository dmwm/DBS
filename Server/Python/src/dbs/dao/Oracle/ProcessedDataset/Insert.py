#!/usr/bin/env python
""" DAO Object for ProcessedDatasets table """ 

__revision__ = "$Revision: 1.5 $"
__version__  = "$Id: Insert.py,v 1.5 2009/11/24 14:31:11 akhukhun Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy import exceptions

class Insert(DBFormatter):
    """ProcessedDataset Insert DAO class"""

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner

        self.sql = \
"""INSERT INTO %sPROCESSED_DATASETS 
(PROCESSED_DS_ID, PROCESSED_DS_NAME) 
VALUES (:processeddsid, :processeddsname)
""" % self.owner

    def execute( self, daoinput, conn=None, transaction=False ):
        """
        daoinput must be validated to have the following keys:
        processeddsid, processeddsname"""

        try:
            self.dbi.processData(self.sql, daoinput, conn, transaction)
        except exceptions.IntegrityError, ex:
            self.logger.warning("Unique constraint violation being ignored...")
            self.logger.warning("%s" % ex)
