#!/usr/bin/env python
""" DAO Object for Files table """ 

__revision__ = "$Revision: 1.4 $"
__version__  = "$Id: Insert.py,v 1.4 2009/11/16 19:24:37 yuyi Exp $ "

from WMCore.Database.DBFormatter import DBFormatter

class Insert(DBFormatter):
    """File Insert DAO Class."""

    def __init__(self, logger, dbi):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % self.dbi.engine.url.username

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
        """
        daoinput must be a (list of) dictionary(is) with the following keys:
        fileid, logicalfilename, isfilevalid, dataset(id),
        block(id), filetype(id), checksum, eventcount, filesize, 
        branchhash(id), adler32, md5, autocrosssection, 
        creationdate, createby, lastmodificationdate, lastmodifiedby
        """

	"""
	really? Can you bin here so one don't have to have the exactly list?
	"""
        self.dbi.processData(self.sql, daoinput, conn, transaction)
