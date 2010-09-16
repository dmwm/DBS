#!/usr/bin/env python
""" DAO Object for Datasets table """ 

__revision__ = "$Revision: 1.15 $"
__version__  = "$Id: Insert.py,v 1.15 2010/07/09 19:38:10 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy import exceptions
from dbs.dao.Oracle.InsertTable.Insert import InsertSingle

class Insert(InsertSingle):
    """ Dataset Insert DAO Class"""
    
    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.logger = logger

#        self.sql = \
#	"""
#	INSERT INTO %sDATASETS 
#		(DATASET_ID, DATASET, IS_DATASET_VALID, 
#			PRIMARY_DS_ID, PROCESSED_DS_ID, DATA_TIER_ID, DATASET_ACCESS_TYPE_ID, 
#			PHYSICS_GROUP_ID, XTCROSSSECTION, GLOBAL_TAG, CREATION_DATE, CREATE_BY, 
#			LAST_MODIFICATION_DATE, LAST_MODIFIED_BY) 
#			VALUES
#			(:dataset_id, :dataset, :is_dataset_valid, 
#			:primary_ds_id, :processed_ds_id, :data_tier_id, :dataset_access_type_id, 
#			:physics_group_id, :xtcrosssection, :global_tag, :creation_date, :create_by, 
#			:last_modification_date, :last_modified_by) """ % self.owner
#
#    def processInput(self, daoinput):
#                #valid_keys = self.sql.split("VALUES")[1].split('(')[1].split(')')[0].replace(",","").strip().split(":")
#                valid_keys = self.sql.split("VALUES")[1].split('(')[1].split(')')[0].replace(',','').replace(':','').strip().split()
#                for akey in daoinput.keys():
#                        if akey not in valid_keys:
#                                del daoinput[akey]
#				self.logger.warning("DROPPING Key: "+akey)
#                return daoinput

    def execute(self, conn, daoinput, transaction = False):

        """
        daoinput must be a dictionary with the following keys:
	dataset_id, dataset, is_dataset_valid, 
	primary_ds_id, processed_ds_id, data_tier_id, dataset_access_type_id, 
	physics_group_id, xtcrosssection, global_tag, creation_date, create_by, 
	last_modification_date, last_modified_by
	"""
	if not conn:
	    raise Exception("dbs/dao/Oracle/Dataset/Insert expects db connection from upper layer.")
	#daoinput=self.processInput(daoinput)
        #self.dbi.processData(self.sql, daoinput, conn, transaction)
	try:
	    self.executeSingle(conn, daoinput, "DATASETS", transaction)
	except Exception:
	    raise
					
