#!/usr/bin/env python
""" DAO Object for Datasets table """ 

__revision__ = "$Revision: 1.10 $"
__version__  = "$Id: Insert.py,v 1.10 2010/01/19 19:44:38 afaq Exp $ "

from WMCore.Database.DBFormatter import DBFormatter
from sqlalchemy import exceptions

class Insert(DBFormatter):
    """ Dataset Insert DAO Class"""
    
    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner
        self.logger = logger

        self.sql = \
	"""
	INSERT INTO %sDATASETS 
		(DATASET_ID, DATASET, IS_DATASET_VALID, 
			PRIMARY_DS_ID, PROCESSED_DS_ID, DATA_TIER_ID, DATASET_TYPE_ID, 
			PHYSICS_GROUP_ID, XTCROSSSECTION, GLOBAL_TAG, CREATION_DATE, CREATE_BY, 
			LAST_MODIFICATION_DATE, LAST_MODIFIED_BY) 
			VALUES
			(:dataset_id, :dataset, :is_dataset_valid, 
			:primary_ds_id, :processed_ds_id, :data_tier_id, :dataset_type_id, 
			:physics_group_id, :xtcrosssection, :global_tag, :creation_date, :create_by, 
			:last_modification_date, :last_modified_by) """ % self.owner

    def processInput(self, daoinput):
                #valid_keys = self.sql.split("VALUES")[1].split('(')[1].split(')')[0].replace(",","").strip().split(":")
                valid_keys = self.sql.split("VALUES")[1].split('(')[1].split(')')[0].replace(',','').replace(':','').strip().split()
                for akey in daoinput.keys():
                        if akey not in valid_keys:
                                del daoinput[akey]
				self.logger.warning("DROPPING Key: "+akey)
                return daoinput

    def execute(self, daoinput, conn = None, transaction = False):

        """
        daoinput must be a dictionary with the following keys:
	dataset_id, dataset, is_dataset_valid, 
	primary_ds_id, processed_ds_id, data_tier_id, dataset_type_id, 
	physics_group_id, xtcrosssection, global_tag, creation_date, create_by, 
	last_modification_date, last_modified_by
	"""
	daoinput=self.processInput(daoinput)
        self.dbi.processData(self.sql, daoinput, conn, transaction)
