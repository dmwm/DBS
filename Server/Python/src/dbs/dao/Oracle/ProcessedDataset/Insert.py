#!/usr/bin/env python
""" DAO Object for ProcessedDatasets table """ 

__revision__ = "$Revision: 1.6 $"
__version__  = "$Id: Insert.py,v 1.6 2009/12/21 21:05:41 afaq Exp $ "

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
		VALUES (:processed_ds_id, :processed_ds_name)
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
