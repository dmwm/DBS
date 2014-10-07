#!/usr/bin/env python
""" DAO Object for Datasets table """ 

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Insert(DBFormatter):
    """ Dataset Insert DAO Class"""
    
    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.logger = logger

        self.sql = \
        """insert all
           when not exists (select * from %sprocessed_datasets where processed_ds_name=processed_n) then
                into %sprocessed_datasets(processed_ds_id, processed_ds_name) values (%sseq_psds.nextval, processed_n)
           when 1 = 1 then  
           into %sdatasets ( dataset_id, dataset, primary_ds_id, processed_ds_id, data_tier_id,
                           dataset_access_type_id, acquisition_era_id,  processing_era_id,
                           physics_group_id,  xtcrosssection, prep_id, creation_date, create_by,
                           last_modification_date, last_modified_by
                         )
                  values ( :dataset_id, :dataset,
                           (select primary_ds_id from %sprimary_datasets where primary_ds_name=pri_n),
                           nvl((select processed_ds_id  from %sprocessed_datasets where processed_ds_name=processed_n),
                                %sseq_psds.nextval),
                           (select data_tier_id from %sdata_tiers where data_tier_name=tier),
                           (select dataset_access_type_id from %sdataset_access_types where dataset_access_type=access_t),
                           :acquisition_era_id, :processing_era_id, :physics_group_id,
                           :xtcrosssection, :prep_id, :creation_date, :create_by,
                           :last_modification_date, :last_modified_by 
                         )
                select  :primary_ds_name pri_n, :processed_ds_name processed_n,
                        :data_tier_name tier,  :dataset_access_type access_t 
                from dual""" %((self.owner,)*9)

    def execute(self, conn, daoinput, transaction = False):

        """
        daoinput must be a dictionary with the following keys:
	dataset_id, dataset, primary_ds_name, processed_ds_name, data_tier_name, dataset_access_types,
	physics_group_name, xtcrosssection, creation_date, create_by, last_modification_date, last_modified_by
	"""
	if not conn:
            dbsExceptionHandler("dbsException-db-conn-failed", "Oracle/Dataset/Insert.  Expects db connection from upper layer.")
        if daoinput == {}:
            # Nothing to do
            return

        #self.executeSingle(conn, daoinput, "DATASETS", transaction)
        result = self.dbi.processData(self.sql, daoinput, conn, transaction)

        return

					
