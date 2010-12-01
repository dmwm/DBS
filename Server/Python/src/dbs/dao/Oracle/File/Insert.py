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
        self.sql = """INSERT INTO %sFILES (FILE_ID, LOGICAL_FILE_NAME, IS_FILE_VALID, 
                        DATASET_ID, BLOCK_ID, FILE_TYPE_ID, CHECK_SUM, EVENT_COUNT, FILE_SIZE,
                        ADLER32, MD5, AUTO_CROSS_SECTION, CREATION_DATE, CREATE_BY,
                        LAST_MODIFICATION_DATE, LAST_MODIFIED_BY)
                      VALUES (:file_id, :logical_file_name, :is_file_valid, :dataset_id, 
                        :block_id, :file_type_id, :check_sum, :event_count, :file_size, 
                        :adler32, :md5, :auto_cross_section, :creation_date, :create_by, 
                        :last_modification_date, :last_modified_by) """ % self.owner

    def formatBinds(self, daoinput):
        """
        _formatBinds_

        Format the binds and deal with optional variables
        """

        binds = []
        for entry in daoinput:
            tmpBind = {}
            tmpBind['file_id']                = entry['file_id']
            tmpBind['logical_file_name']      = entry['logical_file_name']
            tmpBind['dataset_id']             = entry['dataset_id']
            tmpBind['block_id']               = entry['block_id']
            tmpBind['file_type_id']           = entry['file_type_id']
            tmpBind['check_sum']              = entry['check_sum']
            tmpBind['event_count']            = entry['event_count']
            tmpBind['file_size']              = entry['file_size']
            
            # Optional
            tmpBind['is_file_valid']          = entry.get('is_file_valid', 1)
            tmpBind['adler32']                = entry.get('adler32', 'NOTSET')
            tmpBind['md5']                    = entry.get('md5', 'NOTSET')
            tmpBind['auto_cross_section']     = entry.get('auto_cross_section', None)
            tmpBind['creation_date']          = entry.get('creation_date', None)
            tmpBind['create_by']              = entry.get('create_by', None)
            tmpBind['last_modification_date'] = entry.get('last_modification_date', None)
            tmpBind['last_modified_by']       = entry.get('last_modified_by', None)

            binds.append(tmpBind)

        return binds

    def execute(self, conn, daoinput, transaction = False):
	if not conn:
	    raise Exception("dbs/dao/Oracle/File/Insert expects db connection from upper layer.")

        try:
            if type(daoinput) == list:
                binds = self.formatBinds(daoinput = daoinput)
            else:
                binds = daoinput
        except KeyError, ex:
            raise Exception("Missing critical key in binds in Files.Insert: %s" % str(ex))

        print "About to insert file with dataset id"
        print binds[0]['dataset_id']
        
	self.dbi.processData(self.sql, binds, conn, transaction)
