#!/usr/bin/env python
""" DAO Object for OutputModuleConfigs table """ 

from WMCore.Database.DBFormatter import DBFormatter
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class Insert(DBFormatter):

    def __init__(self, logger, dbi, owner):
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % owner if not owner in ("", "__MYSQL__") else ""

        self.sql = \
                """INSERT ALL
                   WHEN not exists(select app_exec_id from %sapplication_executables where app_name = app_n) THEN
                        INTO %sapplication_executables(app_exec_id, app_name)values(%sseq_ae.nextval, app_n)
                   WHEN not exists (select release_version_id from %srelease_versions where release_version = release_v) THEN
                        INTO %srelease_versions(release_version_id, release_version) values (%sseq_rv.nextval, release_v)
                   WHEN not exists(select parameter_set_hash_id from %sparameter_set_hashes where pset_hash = pset_h) THEN
                        INTO %sparameter_set_hashes ( parameter_set_hash_id, pset_hash, pset_name ) values (%sseq_psh.nextval, pset_h, pset_name)
                   WHEN 1=1 THEN
                        INTO %soutput_module_configs ( output_mod_config_id, app_exec_id, release_version_id,
                        parameter_set_hash_id, output_module_label, global_tag, scenario, creation_date, create_by
                        ) values (%sseq_omc.nextval,
                        NVL((select app_exec_id from %sapplication_executables where app_name = app_n),%sseq_ae.nextval),
                        NVL((select release_version_id from %srelease_versions where release_version = release_v), %sseq_rv.nextval),
                        NVL((select parameter_set_hash_id from  %sparameter_set_hashes where pset_hash = pset_h), %sseq_psh.nextval),
                        :output_module_label, :global_tag, :scenario, :creation_date, :create_by)
                   select :app_name app_n, :release_version release_v, :pset_hash pset_h, :pset_name pset_name from dual
                """% ((self.owner,)*17)

    def execute( self, conn, outputModConfigObj, transaction=False ):
        if not conn:
	    dbsExceptionHandler("dbsException-db-conn-failed", "Oracle/OutputModuleConfig/Insert. Expects db connection from upper layer.")
        result = self.dbi.processData(self.sql, outputModConfigObj, conn, transaction)
            
	return


