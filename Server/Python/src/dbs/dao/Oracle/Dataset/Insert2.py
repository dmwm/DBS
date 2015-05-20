#!/usr/bin/env python
""" DAO Object for Datasets table """ 

from WMCore.Database.DBFormatter import DBFormatter
from dbs.dao.Oracle.InsertTable.Insert import InsertSingle
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Insert2(DBFormatter):
    """ Dataset Insert DAO Class"""
    
    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""
        self.logger = logger

        self.sql = \
        """insert all
           when not exists (select * from %sprocessed_datasets where processed_ds_name=processed_n) then
                into %sprocessed_datasets(processed_ds_id, processed_ds_name) values (%sseq_psds.nextval, processed_n)
           when not exists (select * from %sdataset_access_types where dataset_access_type=access_t) then
                into %sdataset_access_types(dataset_access_type_id, dataset_access_type) values (%sseq_dtp.nextval, access_t)
           when exists (select data_tier_id from %sdata_tiers where data_tier_name=tier) then  
           into %sdatasets ( dataset_id, dataset, primary_ds_id, processed_ds_id, data_tier_id,
                           dataset_access_type_id, acquisition_era_id,  processing_era_id,
                           physics_group_id,  xtcrosssection, prep_id, creation_date, create_by,
                           last_modification_date, last_modified_by
                         )
                  values ( :dataset_id, :dataset, :primary_ds_id,
                           nvl((select processed_ds_id  from %sprocessed_datasets where processed_ds_name=processed_n),
                                %sseq_psds.nextval),
                           (select data_tier_id from %sdata_tiers where data_tier_name=tier),
                          nvl((select dataset_access_type_id from %sdataset_access_types where dataset_access_type=access_t), %sseq_dtp.nextval), 
                           :acquisition_era_id, :processing_era_id, :physics_group_id,
                           :xtcrosssection, :prep_id, cdate, cby,
                           :last_modification_date, :last_modified_by 
                         )
                select  :processed_ds_name processed_n,
                        :data_tier_name tier,  :dataset_access_type access_t,  
                        :creation_date cdate, :create_by cby
                from dual""" %((self.owner,)*16)

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

					
