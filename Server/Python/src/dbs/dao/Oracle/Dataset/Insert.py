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

        self.sql = """INSERT INTO %sDATASETS (DATASET_ID, DATASET, IS_DATASET_VALID, 
                        PRIMARY_DS_ID, PROCESSED_DS_ID, DATA_TIER_ID, DATASET_ACCESS_TYPE_ID,
                        PHYSICS_GROUP_ID, XTCROSSSECTION, GLOBAL_TAG, CREATION_DATE, CREATE_BY, 
                        LAST_MODIFICATION_DATE, LAST_MODIFIED_BY) 
                      VALUES
                        (:dataset_id, :dataset, :is_dataset_valid, 
                        :primary_ds_id, :processed_ds_id, :data_tier_id, :dataset_access_type_id, 
                        :physics_group_id, :xtcrosssection, :global_tag, :creation_date, :create_by, 
                        :last_modification_date, :last_modified_by) """ % self.owner

        return

    def formatBinds(self, daoinput):
        """
        _formatBinds_

        Take the input and create binds out of it
        """

        binds = {}
        binds['dataset_id']              = daoinput['dataset_id']
        binds['dataset']                 = daoinput['dataset']
        binds['primary_ds_id']           = daoinput['primary_ds_id']
        binds['processed_ds_id']         = daoinput['processed_ds_id']
        binds['data_tier_id']            = daoinput['data_tier_id']
        binds['dataset_access_type_id']  = daoinput['dataset_access_type_id']
        binds['physics_group_id']        = daoinput['physics_group_id']

        # Optional?
        binds['is_dataset_valid']        = daoinput.get('is_dataset_valid', 1)
        binds['xtcrosssection']          = daoinput.get('xtcrosssection', None)
        binds['global_tag']              = daoinput.get('global_tag', None)
        binds['creation_date']           = daoinput.get('creation_date', None)
        binds['create_by']               = daoinput.get('create_by', None)
        binds['last_modification_date']  = daoinput.get('last_modification_date', None)
        binds['last_modified_by']        = daoinput.get('last_modified_by', None)

        return binds


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
	#try:
	#    self.executeSingle(conn, daoinput, "DATASETS", transaction)
	#except Exception:
	#    raise

        if daoinput == {}:
            # Nothing to do
            return

        try:
            binds = self.formatBinds(daoinput = daoinput)
            print "About to insert dataset"
            print binds
        except KeyError, ex:
            raise Exception("Missing required input field: %s" % str(ex))
        
        self.dbi.processData(self.sql, binds, conn, transaction)
        return
					
