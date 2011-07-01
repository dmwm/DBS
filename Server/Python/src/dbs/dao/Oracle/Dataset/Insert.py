#!/usr/bin/env python
""" DAO Object for Datasets table """ 

__revision__ = "$Revision: 1.15 $"
__version__  = "$Id: Insert.py,v 1.15 2010/07/09 19:38:10 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy import exceptions
from dbs.dao.Oracle.InsertTable.Insert import InsertSingle
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Insert(InsertSingle):
    """ Dataset Insert DAO Class"""
    
    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.logger = logger

    def execute(self, conn, daoinput, transaction = False):

        """
        daoinput must be a dictionary with the following keys:
	dataset_id, dataset, is_dataset_valid, 
	primary_ds_id, processed_ds_id, data_tier_id, dataset_access_type_id, 
	physics_group_id, xtcrosssection, creation_date, create_by, 
	last_modification_date, last_modified_by
	"""
	if not conn:
            dbsExceptionHandler("dbsException-db-conn-failed", "Oracle/Dataset/Insert.  Expects db connection from upper layer.")
        if daoinput == {}:
            # Nothing to do
            return

        self.executeSingle(conn, daoinput, "DATASETS", transaction)
        
					
