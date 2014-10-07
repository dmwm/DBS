#!/usr/bin/env python
""" DAO Object for Blocks table """ 
from WMCore.Database.DBFormatter import DBFormatter
from dbs.dao.Oracle.InsertTable.Insert import InsertSingle
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Insert(InsertSingle):
    """Block Insert DAO Class"""

    def execute( self, conn, daoinput,  transaction = False):
        """daoinput must be validated to have the following keys:
        block_id, block_name, dataset_id
	It May have the following keys:
	open_for_writing, origin_site(id), block_size,
        file_count, creation_date, create_by, lastmodification_date, lastmodified_by
        """
	if not conn:
            dbsExceptionHandler("dbsException-db-conn-failed", "Oracle/Block/Insert.  Expects db connection from upper layer.")
        self.executeSingle(conn, daoinput, "BLOCKS", transaction)
            

