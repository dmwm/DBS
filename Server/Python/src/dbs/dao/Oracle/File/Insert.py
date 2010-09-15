#!/usr/bin/env python
""" DAO Object for Files table """ 

__revision__ = "$Revision: 1.8 $"
__version__  = "$Id: Insert.py,v 1.8 2009/11/27 09:55:03 akhukhun Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy import exceptions

class Insert(DBFormatter):
    """File Insert DAO Class."""

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner
        self.logger = logger
        self.sql = \
"""
INSERT INTO %sFILES 
(FILE_ID, LOGICAL_FILE_NAME, IS_FILE_VALID, 
DATASET_ID, BLOCK_ID, FILE_TYPE_ID, 
CHECK_SUM, EVENT_COUNT, FILE_SIZE, 
ADLER32, MD5, AUTO_CROSS_SECTION, CREATION_DATE, CREATE_BY, 
LAST_MODIFICATION_DATE, LAST_MODIFIED_BY) 
VALUES (:FILE_ID, :LOGICAL_FILE_NAME, :IS_FILE_VALID, :DATASET, 
:BLOCK, :FILE_TYPE, :CHECK_SUM, :EVENT_COUNT, :FILE_SIZE, 
:ADLER32, :MD5, :AUTO_CROSS_SECTION, :CREATION_DATE, :CREATE_BY, 
:LAST_MODIFICATION_DATE, :LAST_MODIFIED_BY) 
""" % self.owner

    def execute(self, daoinput, conn = None, transaction = False):
        try:
            self.dbi.processData(self.sql, daoinput, conn, transaction)
        except exceptions.IntegrityError, ex:
            self.logger.warning("Unique constraint violation being ignored...")
            self.logger.warning("%s" % ex)
