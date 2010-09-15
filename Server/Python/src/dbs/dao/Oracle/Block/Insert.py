#!/usr/bin/env python
""" DAO Object for Blocks table """ 

__revision__ = "$Revision: 1.6 $"
__version__  = "$Id: Insert.py,v 1.6 2009/11/24 14:31:10 akhukhun Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy import exceptions

class Insert(DBFormatter):
    """Block Insert DAO Class"""

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner
        self.sql = \
"""
INSERT INTO %sBLOCKS 
(BLOCK_ID, BLOCK_NAME, DATASET_ID, OPEN_FOR_WRITING, ORIGIN_SITE, BLOCK_SIZE, 
FILE_COUNT, CREATION_DATE, CREATE_BY, LAST_MODIFICATION_DATE, LAST_MODIFIED_BY) 
VALUES(:blockid, :blockname, :dataset, :openforwriting, :originsite, :blocksize, 
:filecount, :creationdate, :createby, :lastmodificationdate, :lastmodifiedby)
""" % (self.owner)

    def execute( self, daoinput, conn = None, transaction = False):
        """daoinput must be validated to have the following keys:
        blockid, blockname, dataset(id), openforwriting, originsite(id), blocksize,
        filecount, creationdate, createby, lastmodificationdate, lastmodifiedby
        """
        try:
            self.dbi.processData(self.sql, daoinput, conn, transaction)
        except exceptions.IntegrityError, ex:
            self.logger.warning("Unique constraint violation being ignored...")
            self.logger.warning("%s" % ex)

