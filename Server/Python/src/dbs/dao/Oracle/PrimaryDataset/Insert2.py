#!/usr/bin/env python
""" DAO Object for insert into Primary_datasts table when calling block bulk insertion  """ 

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Insert2(DBFormatter):

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""

        self.sql = \
                """insert all
                    when not exists (select * from %sprimary_ds_types where primary_ds_type = primary_ds_t) then
                         into %sprimary_ds_types(primary_ds_type_id, primary_ds_type) values (%sseq_pdt.nextval, primary_ds_t)
                    when 1 = 1 then
                         into %sprimary_datasets(primary_ds_id, primary_ds_name, primary_ds_type_id, creation_date, create_by)
                         values(:primary_ds_id, :primary_ds_name,
                         nvl((select primary_ds_type_id from %sprimary_ds_types where primary_ds_type = primary_ds_t),%sseq_pdt.nextval),
                         :creation_date, :create_by)
                    select :primary_ds_type primary_ds_t from dual
                """% ((self.owner,)*6)

    def execute( self, conn, inputObj, transaction=False ):
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed", "PrimaryDataset/Insert2. Expects db connection from upper layer.")
            
        result = self.dbi.processData(self.sql, inputObj, conn, transaction)
            
	return


