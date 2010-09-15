#!/usr/bin/env python
""" DAO Object for Blocks table """ 

__revision__ = "$Revision: 1.3 $"
__version__  = "$Id: Insert.py,v 1.3 2009/10/27 17:24:48 akhukhun Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):
    """Block Insert DAO Class"""

    def __init__(self, logger, dbi):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % self.dbi.engine.url.username

        self.sql = \
"""
INSERT INTO %sBLOCKS 
(BLOCK_ID, BLOCK_NAME, DATASET_ID, OPEN_FOR_WRITING, ORIGIN_SITE, BLOCK_SIZE, 
FILE_COUNT, CREATION_DATE, CREATE_BY, LAST_MODIFICATION_DATE, LAST_MODIFIED_BY) 
VALUES(:blockid, :blockname, :datasetid, :openforwriting, :originsite, :blocksize, 
:filecount, :creationdate, :createby, :lastmodificationdate, :lastmodifiedby)
""" % (self.owner)

    def execute( self, daoinput, conn = None, transaction = False):
        """daoinput must be validated to have the following keys:
        blockid, blockname, datasetid, openforwriting, originsite, blocksize,
        filecount, creationdate, createby, lastmodificationdate, lastmodifiedby
        """
        self.dbi.processData(self.sql, daoinput, conn, transaction)

