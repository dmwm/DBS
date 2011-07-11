#!/usr/bin/env python
#pylint: disable=C0103
"""
This module provides business object class to interact with Dataset. 
"""
import cjson
from WMCore.DAOFactory import DAOFactory
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

class DBSDataset:
    """
    Dataset business object class
    """
    def __init__(self, logger, dbi, owner):
        """
        initialize business object class.
        """
        daofactory = DAOFactory(package='dbs.dao', logger=logger, 
                                dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        
        self.datasetlist = daofactory(classname="Dataset.List")
        self.datasetbrieflist = daofactory(classname="Dataset.BriefList")
        self.datasetid = daofactory(classname="Dataset.GetID")
        self.sm = daofactory(classname="SequenceManager")
        self.primdsid = daofactory(classname='PrimaryDataset.GetID')
        self.tierid = daofactory(classname='DataTier.GetID')
        self.datatypeid = daofactory(classname='DatasetType.GetID')
        self.phygrpid = daofactory(classname='PhysicsGroup.GetID')
        self.procdsid = daofactory(classname='ProcessedDataset.GetID')
        self.procdsin = daofactory(classname='ProcessedDataset.Insert')
        self.datasetin = daofactory(classname='Dataset.Insert')
        self.outconfigid = daofactory(classname='OutputModuleConfig.GetID')
        self.datasetoutmodconfigin = daofactory(classname=
                                            'DatasetOutputMod_config.Insert')
        self.proceraid = daofactory(classname='ProcessingEra.GetID')
        self.acqeraid = daofactory(classname='AcquisitionEra.GetID')
        self.updatestatus = daofactory(classname='Dataset.UpdateStatus')
        self.updatetype = daofactory(classname='Dataset.UpdateType')
        self.datasetparentlist = daofactory(classname="DatasetParent.List")
        self.datasetchildlist = daofactory(classname="DatasetParent.ListChild")

    def listDatasetParents(self, dataset=""):
        """
        takes required dataset parameter
        returns only parent dataset name
        """
        if( dataset == "" ):
            dbsExceptionHandler("dbsException-invalid-input", "DBSDataset/listDatasetParents. Child Dataset name is required.")
        try:
            conn = self.dbi.connection()
            result = self.datasetparentlist.execute(conn, dataset)
            return result
        except Exception, ex:
            raise ex
        finally:
            conn.close()

    def listDatasetChildren(self, dataset):
        """
        takes required dataset parameter
        returns only children dataset name
        """
        if( dataset == "" ):
            dbsExceptionHandler("dbsException-invalid-input", "DBSDataset/listDatasetChildren. Parent Dataset name is required.")
        try:
            conn = self.dbi.connection()
            result = self.datasetchildlist.execute(conn, dataset)
            return result
        except Exception, ex:
            raise ex
        finally:
            conn.close()

    def updateStatus(self, dataset, is_dataset_valid):
        """
        Used to toggle the status of a dataset  is_dataset_valid=0/1 (invalid/valid)
        """
        conn = self.dbi.connection()
        trans = conn.begin()

        try:
            self.updatestatus.execute(conn, dataset, is_dataset_valid, trans)
            trans.commit()
        except Exception, ex:
            trans.rollback()
            raise ex
        finally:
            trans.close()
            conn.close()
    
    def updateType(self, dataset, dataset_access_type):
        """
        Used to change the status of a dataset type (production/etc.)
        """
        conn = self.dbi.connection()
        trans = conn.begin()

        try :
            self.updatetype.execute(conn, dataset, dataset_access_type.upper(), trans)
            trans.commit()
        except Exception, ex:
            trans.rollback()
            raise ex
        finally:
            trans.close()
            conn.close()
   
    def listDatasets(self, dataset="", parent_dataset="", is_dataset_valid=1,
                     release_version="", pset_hash="", app_name="",
                     output_module_label="", processing_version="",
                     acquisition_era="", run_num=0, physics_group_name="",
                     logical_file_name="", primary_ds_name="",
                     primary_ds_type="", data_tier_name="",
                     dataset_access_type="RO", min_cdate=0, max_cdate=0,
                     min_ldate=0, max_ldate=0, cdate=0, ldate=0, detail=False):
        """
        lists all datasets if dataset parameter is not given.
        The parameter can include % character. 
        all other parameters are not wild card ones.
        """
        #import pdb
        #pdb.set_trace()
        #self.logger.warning("I am in listDataset businese")
        if(logical_file_name and logical_file_name.find("%")!=-1):
            dbsExceptionHandler('dbsException-invalid-input', 'DBSDataset/listDatasets API requires \
                fullly qualified logical_file_name. NO wildcard is allowed in logical_file_name.')
        try:
            conn = None
            conn = self.dbi.connection()

            dao = (self.datasetbrieflist, self.datasetlist)[detail]

            result = dao.execute(conn, 
                                 dataset, is_dataset_valid,
                                 parent_dataset,
                                 release_version,
                                 pset_hash,
                                 app_name,
                                 output_module_label,
                                 processing_version.upper(),
                                 acquisition_era.upper(), 
                                 run_num, physics_group_name,
                                 logical_file_name,
                                 primary_ds_name, primary_ds_type,
                                 data_tier_name.upper(),
                                 dataset_access_type.upper(),
                                 min_cdate, max_cdate, min_ldate, max_ldate,
                                 cdate, ldate)    
            return result
        except Exception, ex:
            raise ex
        finally:
            if conn:
                conn.close()

    def listDatasetArray(self, inputdata=None):
        if not inputdata:
            dbsExceptionHandler('dbsException-invalid-input', 'DBSDataset/listDatasetArray API requires \
                at least a list of dataset.')
        else:
            try:
                dataset = inputdata["dataset"]
                if inputdata.has_key("is_dataset_valid"):
                    is_dataset_valid = inputdata["is_dataset_valid"]
                else:
                    is_dataset_valid = 1
                if inputdata.has_key("dataset_access_type"):
                    dataset_access_type = inputdata["dataset_access_type"]
                else:
                    dataset_access_type = "RO" 
                if inputdata.has_key("detail"):
                    detail = inputdata["detail"]
                else:
                    detail = False 
                conn = None
                conn = self.dbi.connection()
                #import pdb
                #pdb.set_trace()

                dao = (self.datasetbrieflist, self.datasetlist)[detail]   
                result = dao.execute(conn, dataset=dataset, is_dataset_valid=is_dataset_valid,
                    dataset_access_type=dataset_access_type, transaction=False)
                return result                        
            except cjson.DecodeError, de:
                msg = "business/listDatasetArray requires at least a list of dataset. %s" % de
                dbsExceptionHandler('dbsException-invalid-input', msg)
            except Exception, ex:
                raise ex
            finally:
                if conn:
                    conn.close()
    
    def insertDataset(self, businput):
        """
        input dictionary must have the following keys:
        dataset, is_dataset_valid, primary_ds_name(name), processed_ds(name), data_tier(name),
        dataset_access_type(name), acquisition_era(name), processing_version(name), 
        physics_group(name), xtcrosssection, creation_date, create_by, 
        last_modification_date, last_modified_by
        """ 
        if not (businput.has_key("primary_ds_name") and businput.has_key("dataset")
                and businput.has_key("is_dataset_valid") and businput.has_key("processed_ds_name") ):
            dbsExceptionHandler('dbsException-invalid-input', "business/DBSDataset/insertDataset must have dataset,\
                is_dataset_valid, primary_ds_name, processed_ds_name as input")

        if not (businput.has_key("data_tier_name") and businput.has_key("dataset_access_type") ):
            dbsExceptionHandler('dbsException-invalid-input', "business/DBSDataset/insertDataset must have data_tier(name),\
                dataset_access_type as input.")

        conn = self.dbi.connection()
        tran = conn.begin()
        try:

            dsdaoinput = {}
            dsdaoinput["primary_ds_id"] = self.primdsid.execute(conn, businput["primary_ds_name"], tran)
            if dsdaoinput["primary_ds_id"] == -1:
                dbsExceptionHandler("dbsException-missing-data", "DBSDataset/insertDataset. Primary Dataset: %s not found"
                                                                                    % businput["primary_ds_name"]) 
            dsdaoinput["data_tier_id"] = self.tierid.execute(conn, businput["data_tier_name"].upper(), tran)
            if dsdaoinput["data_tier_id"] == -1:
                dbsExceptionHandler("dbsException-missing-data", "DBSDataset/insertDataset. Data Tier: %s not found"
                                                                                    % businput["data_tier_name"]) 
            dsdaoinput["dataset_access_type_id"] = self.datatypeid.execute(conn, businput["dataset_access_type"].upper(), tran)
            if dsdaoinput["dataset_access_type_id"] == -1:
                dbsExceptionHandler("dbsException-missing-data", "DBSDataset/insertDataset. Dataset Access Type : %s not found"
                                                                                    % businput["dataset_access_type"] )
            dsdaoinput["physics_group_id"] = self.phygrpid.execute(conn, businput["physics_group_name"], tran)
            if dsdaoinput["physics_group_id"]  == -1:
                dbsExceptionHandler("dbsException-missing-data",  "DBSDataset/insertDataset. Physics Group : %s Not found"
                                                                                    % businput["physics_group_name"]) 

            # See if processed dataset exists, if not, add one
            procid = self.procdsid.execute(conn, businput["processed_ds_name"],
                                            tran)
            if procid > 0:
                dsdaoinput["processed_ds_id"] = procid
            else:
                procid = self.sm.increment(conn, "SEQ_PSDS", tran)
                procdaoinput = {
                    "processed_ds_name" : businput["processed_ds_name"],
                    "processed_ds_id" : procid}
                self.procdsin.execute(conn, procdaoinput, tran)
                dsdaoinput["processed_ds_id"] = procid

            dsdaoinput["dataset_id"] = self.sm.increment(conn, "SEQ_DS", tran)

            # we are better off separating out what we need for the dataset DAO
            dsdaoinput.update({ 
                               "dataset" : "/%s/%s/%s" %
                               (businput["primary_ds_name"],
                                businput["processed_ds_name"],
                                businput["data_tier_name"].upper()),
                               "is_dataset_valid" :
                                    businput["is_dataset_valid"],
                               "creation_date" : businput["creation_date"],
                               "xtcrosssection" : businput["xtcrosssection"],
                               "create_by" : businput["create_by"],
                               "last_modification_date" :
                                    businput["last_modification_date"],
                               "last_modified_by" :
                                    businput["last_modified_by"]})

            # See if Processing Era exists
            if businput.has_key("processing_version"):
                dsdaoinput["processing_era_id"] = self.proceraid.execute(conn, businput["processing_version"].upper(), tran)
                if dsdaoinput["processing_era_id"] == -1 :
                    dbsExceptionHandler("dbsException-missing-data", "DBSDataset/insertDataset. Processing Era : %s not found"
                                                        % businput["processing_version"]) 
            # See if Acquisition Era exists
            if businput.has_key("acquisition_era_name"):
                dsdaoinput["acquisition_era_id"] = self.acqeraid.execute(conn, businput["acquisition_era_name"].upper(), tran)
                if dsdaoinput["acquisition_era_id"] == -1 :
                    dbsExceptionHandler("dbsException-missing-data", "DBSDataset/insertDataset. Acquisition Era : %s not found"
                                                        % dsdaoinput["acquisition_era_id"])
                 
            try:
                # insert the dataset
                self.datasetin.execute(conn, dsdaoinput, tran)
            except Exception, ex:
                if (str(ex).lower().find("unique constraint") != -1 or
                    str(ex).lower().find("duplicate") != -1):
                    # dataset already exists, lets fetch the ID
                    self.logger.warning(
                            "Unique constraint violation being ignored...")
                    self.logger.warning("%s" % ex)
                    ds = "/%s/%s/%s" % (businput["primary_ds_name"], businput["processed_ds_name"], businput["data_tier_name"].upper())
                    dsdaoinput["dataset_id"] = self.datasetid.execute(conn, ds , tran)
                    if dsdaoinput["dataset_id"] == -1 :
                        dbsExceptionHandler("dbsException-missing-data", "DBSDataset/insertDataset. Strange error, the dataset %s does not exist ?" 
                                                    % ds )
                else:
                    raise       

            #FIXME : What about the READ-only status of the dataset
            # Create dataset_output_mod_mod_configs mapping
            if businput.has_key("output_configs"):
                for anOutConfig in businput["output_configs"]:
                    dsoutconfdaoin = {}
                    dsoutconfdaoin["dataset_id"] = dsdaoinput["dataset_id"]
                    dsoutconfdaoin["output_mod_config_id"] = self.outconfigid.execute(conn, anOutConfig["app_name"],
                                                                                anOutConfig["release_version"],
                                                                                anOutConfig["pset_hash"],
                                                                                anOutConfig["output_module_label"],
                                                                                anOutConfig["global_tag"], tran) 
                    if dsoutconfdaoin["output_mod_config_id"] == -1 : 
                        dbsExceptionHandler("dbsException-missing-data", "DBSDataset/insertDataset: Output config (%s, %s, %s, %s, %s) not found"
                                                                                % (anOutConfig["app_name"],
                                                                                   anOutConfig["release_version"],
                                                                                   anOutConfig["pset_hash"],
                                                                                   anOutConfig["output_module_label"],
                                                                                   anOutConfig["global_tag"]))
                    dsoutconfdaoin["ds_output_mod_conf_id"] = self.sm.increment(conn, "SEQ_DC", tran)
                    try:
                        self.datasetoutmodconfigin.execute(conn, dsoutconfdaoin, tran)
                    except Exception, ex:
                        if str(ex).lower().find("unique constraint") != -1 or str(ex).lower().find("duplicate") != -1:
                            pass
                        else:
                            raise
            # Dataset parentage will NOT be added by this API it will be set by insertFiles()--deduced by insertFiles
            # Dataset  runs will NOT be added by this API they will be set by insertFiles()--deduced by insertFiles OR insertRun API call
            tran.commit()
        except Exception:
            tran.rollback()
            raise
        finally:
            conn.close()
