#!/usr/bin/env python
""" DAO Object for Files table """ 

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Insert2(DBFormatter):
    """File Insert DAO Class."""
    #This dao is not how the other daos in DBS3 written. We'd rather leave the details to be deal by business layer
    #instead of in the dao layer. DAOs should be built to accept whatever the business want it to do. YG 11/17/2010 

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.logger = logger
        self.sql = \
        """insert all
           when not exists (select * from %sfile_data_types where file_type = file_t) then
                into %sfile_data_types(file_type_id, file_type) values( %sseq_ft.nextval, file_t )
           when 1 = 1 then
                into %sfiles (file_id, logical_file_name, is_file_valid, 
                        dataset_id, block_id, file_type_id, check_sum, event_count, file_size,
                        adler32, md5, auto_cross_section,
                        last_modification_date, last_modified_by)
                values (:file_id, :logical_file_name, :is_file_valid, :dataset_id, :block_id, 
                        nvl(( select file_type_id from %sfile_data_types where file_type = file_t ), %sseq_ft.nextval ),  
                        :check_sum, :event_count, :file_size, 
                        :adler32, :md5, :auto_cross_section, 
                        :last_modification_date, :last_modified_by) 
         select :file_type file_t from dual               
       """ % ((self.owner,)*6)
                        

#Move these part to business layer. YG 11/23/2010
    #def formatBinds(self, daoinput):
        """
        _formatBinds_

        Format the binds and deal with optional variables
        """

    def execute(self, conn, daoinput, transaction = False):
	print("************************")
        print(self.sql)
        print(daoinput)    
        print("************************")        
        self.dbi.processData(self.sql, daoinput, conn, transaction)
        
