#!/usr/bin/env python
""" DAO Object for Files table """ 

__revision__ = "$Revision: 1.14 $"
__version__  = "$Id: Insert.py,v 1.14 2010/06/23 21:21:23 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy import exceptions

class Insert(DBFormatter):
    """File Insert DAO Class."""

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.logger = logger
        self.sql = \
"""
INSERT INTO %sFILES 
(FILE_ID, LOGICAL_FILE_NAME, IS_FILE_VALID, 
DATASET_ID, BLOCK_ID, FILE_TYPE_ID, 
CHECK_SUM, EVENT_COUNT, FILE_SIZE, 
ADLER32, MD5, AUTO_CROSS_SECTION, CREATION_DATE, CREATE_BY, 
LAST_MODIFICATION_DATE, LAST_MODIFIED_BY) 
VALUES (:file_id, :logical_file_name, :is_file_valid, :dataset_id, 
:block_id, :file_type_id, :check_sum, :event_count, :file_size, 
:adler32, :md5, :auto_cross_section, :creation_date, :create_by, 
:last_modification_date, :last_modified_by) 
""" % self.owner

    def execute(self, conn, daoinput, transaction = False):
	if not conn:
	    raise Exception("dbs/dao/Oracle/File/Insert expects db connection from upper layer.")
	self.dbi.processData(self.sql, daoinput, conn, transaction)
