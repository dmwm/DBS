#!/usr/bin/env python
""" DAO Object for Files table """ 

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Insert(DBFormatter):
    """File Insert DAO Class."""
    #This dao is not how the other daos in DBS3 written. We'd rather leave the details to be deal by business layer
    #instead of in the dao layer. DAOs should be built to accept whatever the business want it to do. YG 11/17/2010 

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.logger = logger
        self.sql = """INSERT INTO %sFILES (FILE_ID, LOGICAL_FILE_NAME, IS_FILE_VALID, 
                        DATASET_ID, BLOCK_ID, FILE_TYPE_ID, CHECK_SUM, EVENT_COUNT, FILE_SIZE,
                        ADLER32, MD5, AUTO_CROSS_SECTION,
                        LAST_MODIFICATION_DATE, LAST_MODIFIED_BY)
                      VALUES (:file_id, :logical_file_name, :is_file_valid, :dataset_id, 
                        :block_id, :file_type_id, :check_sum, :event_count, :file_size, 
                        :adler32, :md5, :auto_cross_section, 
                        :last_modification_date, :last_modified_by) """ % self.owner
                        

#Move these part to business layer. YG 11/23/2010
    #def formatBinds(self, daoinput):
        """
        _formatBinds_

        Format the binds and deal with optional variables
        """

    def execute(self, conn, daoinput, transaction = False):
	
        #print "About to insert file with dataset id"
        #print binds[0]['dataset_id']
        self.dbi.processData(self.sql, daoinput, conn, transaction)
        
