#!/usr/bin/env python
"""
This module provides business object class to interact with Dataset. 
"""

__revision__ = "$Id: DBSDataset.py,v 1.14 2010/01/01 19:01:33 akhukhun Exp $"
__version__ = "$Revision: 1.14 $"

from WMCore.DAOFactory import DAOFactory

from exceptions import Exception

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
        self.datasetlist1 = daofactory(classname="Dataset.List1")
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
        self.datasetoutmodconfigin = daofactory(classname='DatasetOutputMod_config.Insert')
        self.proceraid= daofactory(classname='ProcessingEra.GetID')
        self.acqeraid = daofactory(classname='AcquisitionEra.GetID')

    def listDatasets(self, dataset="", parent_dataset="", version="",
                     hash="", app_name="", output_module_label=""):
        """
        lists all datasets if dataset parameter is not given.
        The parameter can include % character. 
        all other parameters are not wild card ones.
        """
        if not (parent_dataset or version or hash or app_name or output_module_label):
            return self.datasetlist.execute(dataset)
        else:
            return self.datasetlist1.execute(dataset, 
                                             parent_dataset, 
                                             version, 
                                             app_name, 
                                             output_module_label)
            
    
    def insertDataset(self, businput):
        """
        input dictionary must have the following keys:
        dataset, is_dataset_valid, primary_ds_name(name), processed_ds(name), data_tier(name),
        dataset_type(name), acquisition_era(name), processing_version(name), 
        physics_group(name), xtcrosssection, global_tag, creation_date, create_by, 
        last_modification_date, last_modified_by
        """ 
        conn = self.dbi.connection()
        tran = conn.begin()
        try:

            dsdaoinput={}
            dsdaoinput["primary_ds_id"] = self.primdsid.execute(businput["primary_ds_name"], conn, True)
            dsdaoinput["data_tier_id"] = self.tierid.execute(businput["data_tier_name"], conn, True)
            dsdaoinput["dataset_type_id"] = self.datatypeid.execute(businput["dataset_type"], conn, True)
            dsdaoinput["physics_group_id"] = self.phygrpid.execute(businput["physics_group_name"], conn, True)

            # See if processed dataset exists, if not, add one
            procid = self.procdsid.execute(businput["processed_ds_name"])
            if procid > 0:
                dsdaoinput["processed_ds_id"] = procid
            else:
                procid = self.sm.increment("SEQ_PSDS", conn, True)
                procdaoinput = {"processed_ds_name":businput["processed_ds_name"],
                                    "processed_ds_id":procid}
                self.procdsin.execute(procdaoinput, conn, True)
                dsdaoinput["processed_ds_id"] = procid

            dsdaoinput["dataset_id"] = self.sm.increment("SEQ_DS", conn, True) 

            # we are better off separating out what we need for the dataset DAO
            dsdaoinput.update({ 
                               "dataset" : businput["dataset"],
                               "is_dataset_valid" : businput["is_dataset_valid"],
                               "creation_date" : businput["creation_date"],
                               "xtcrosssection" : businput["xtcrosssection"],
                               "global_tag" : businput["global_tag"],
                               "create_by" : businput["create_by"],
                               "last_modification_date" : businput["last_modification_date"] ,
                               "last_modified_by" : businput["last_modified_by"]})

            # See if Processing Era exists
            if businput.has_key("processing_version"):
                dsdaoinput["processing_version"] = self.proceraid.execute(businput["processing_version"], conn, True)
            # See if Acquisition Era exists
            if businput.has_key("acquisition_era_name"):
                dsdaoinput["acquisition_era_name"] = self.acqeraid.execute(businput["acquisition_era_name"], conn, True)
                 
            try:
                # insert the dataset
                self.datasetin.execute(dsdaoinput, conn, True)
            except Exception, ex:
                if str(ex).lower().find("unique constraint") != -1 :
                    # dataset already exists, lets fetch the ID
                    self.logger.warning("Unique constraint violation being ignored...")
                    self.logger.warning("%s" % ex)
                    dsdaoinput["dataset_id"] = self.datasetid.execute(businput["dataset"], conn, True)
                else:
                    raise	

            #FIXME : What about the READ-only status of the dataset
            # Create dataset_output_mod_mod_configs mapping
            if businput.has_key("output_configs"):
                for anOutConfig in businput["output_configs"]:
                    dsoutconfdaoin={}
                    dsoutconfdaoin["dataset_id"]=dsdaoinput["dataset_id"]
                    dsoutconfdaoin["output_mod_config_id"] = self.outconfigid.execute(anOutConfig["app_name"], \
										anOutConfig["version"], anOutConfig["hash"], conn, True) 
                    dsoutconfdaoin["ds_output_mod_conf_id"]=self.sm.increment("SEQ_DC", conn, True)
                try:
                    self.datasetoutmodconfigin.execute(dsoutconfdaoin, conn, True)
                except Exception, ex:
                    if str(ex).lower().find("unique constraint") != -1 :
                        pass
                    else:
                        raise
            # Dataset parentage will NOT be added by this API it will be set by insertFiles()--deduced by insertFiles
            # Dataset  runs will NOT be added by this API they will be set by insertFiles()--deduced by insertFiles OR insertRun API call
            tran.commit()
        except Exception, e:
            tran.rollback()
            self.logger.exception(e)
            raise
        finally:
            conn.close()
