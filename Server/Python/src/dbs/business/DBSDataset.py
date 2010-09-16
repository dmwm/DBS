#!/usr/bin/env python
"""
This module provides business object class to interact with Dataset. 
"""

__revision__ = "$Id: DBSDataset.py,v 1.42 2010/08/03 21:06:23 afaq Exp $"
__version__ = "$Revision: 1.42 $"

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
        self.datasetoutmodconfigin = daofactory(classname='DatasetOutputMod_config.Insert')
        self.proceraid= daofactory(classname='ProcessingEra.GetID')
        self.acqeraid = daofactory(classname='AcquisitionEra.GetID')
        self.updatestatus = daofactory(classname='Dataset.UpdateStatus')
        self.updatetype = daofactory(classname='Dataset.UpdateType')
        self.datasetparentlist = daofactory(classname="DatasetParent.List")
        self.datasetchildlist = daofactory(classname="DatasetParent.ListChild")

    def listDatasetParents(self, dataset):
        """
        takes required dataset parameter
        returns only parent dataset name
        """
	try:
	    conn = self.dbi.connection()
	    result=self.datasetparentlist.execute(conn, dataset)
	    conn.close()
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
	try:
	    conn = self.dbi.connection()
	    result=self.datasetchildlist.execute(conn, dataset)
	    conn.close()
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

        try :
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
            self.updatetype.execute(conn, dataset, dataset_access_type, trans)
	    trans.commit()
	except Exception, ex:
	    trans.rollback()
            raise ex
	finally:
	    trans.close()
            conn.close()
   
    def listDatasets(self, dataset="", parent_dataset="", release_version="",
                     pset_hash="", app_name="", output_module_label="",
		     processing_version="", acquisition_era="", 
		     run_num=0, physics_group_name="", logical_file_name="", primary_ds_name="",
                     primary_ds_type="", data_tier_name="", dataset_access_type="", detail=False):
        """
        lists all datasets if dataset parameter is not given.
        The parameter can include % character. 
        all other parameters are not wild card ones.
        """
 	if(logical_file_name and logical_file_name.find("%")!=-1):
	    raise Exception("listDataset API only works with fullly qualified logical_file_name. NO * is allowed in logical_file_name.")
	try:
	    conn = self.dbi.connection()

	    dao = (self.datasetbrieflist, self.datasetlist)[detail]

	    result = dao.execute(conn, 
				    dataset,
				    parent_dataset,
                                    release_version,
				    pset_hash,
                                    app_name,
                                    output_module_label,
                                    processing_version,
                                    acquisition_era, 
                                    run_num, physics_group_name, logical_file_name, 
                                    primary_ds_name, primary_ds_type, data_tier_name, 
                                    dataset_access_type)	
	    conn.close()
	    return result
        except Exception, ex:
	    raise ex
        finally:
	    conn.close()
 
    
    def insertDataset(self, businput):
        """
        input dictionary must have the following keys:
        dataset, is_dataset_valid, primary_ds_name(name), processed_ds(name), data_tier(name),
        dataset_access_type(name), acquisition_era(name), processing_version(name), 
        physics_group(name), xtcrosssection, global_tag, creation_date, create_by, 
        last_modification_date, last_modified_by
        """ 
        conn = self.dbi.connection()
        tran = conn.begin()
        try:

            dsdaoinput={}
            dsdaoinput["primary_ds_id"] = self.primdsid.execute(conn, businput["primary_ds_name"], tran)
	    if dsdaoinput["primary_ds_id"] == -1: raise Exception ("Primary Dataset: %s not found" %businput["primary_ds_name"])
            dsdaoinput["data_tier_id"] = self.tierid.execute(conn, businput["data_tier_name"], tran)
	    if dsdaoinput["data_tier_id"] == -1: raise Exception ("Data Tier: %s not found" %businput["data_tier_name"])
            dsdaoinput["dataset_access_type_id"] = self.datatypeid.execute(conn, businput["dataset_access_type"], tran)
	    if dsdaoinput["dataset_access_type_id"] == -1: raise Exception ("Dataset Access Type : %s not found" %businput["dataset_access_type"])
            dsdaoinput["physics_group_id"] = self.phygrpid.execute(conn, businput["physics_group_name"], tran)
	    if dsdaoinput["physics_group_id"]  == -1: raise Exception ("Physics Group : %s Not found" % businput["physics_group_name"])

            # See if processed dataset exists, if not, add one
            procid = self.procdsid.execute(conn, businput["processed_ds_name"], tran)
            if procid > 0:
                dsdaoinput["processed_ds_id"] = procid
            else:
                procid = self.sm.increment(conn, "SEQ_PSDS", tran)
                procdaoinput = {"processed_ds_name":businput["processed_ds_name"],
                                    "processed_ds_id":procid}
                self.procdsin.execute(conn, procdaoinput, tran)
                dsdaoinput["processed_ds_id"] = procid

            dsdaoinput["dataset_id"] = self.sm.increment(conn, "SEQ_DS", tran) 

            # we are better off separating out what we need for the dataset DAO
            dsdaoinput.update({ 
                               "dataset" : "/%s/%s/%s" %(businput["primary_ds_name"], businput["processed_ds_name"], businput["data_tier_name"]),
                               "is_dataset_valid" : businput["is_dataset_valid"],
                               "creation_date" : businput["creation_date"],
                               "xtcrosssection" : businput["xtcrosssection"],
                               "global_tag" : businput["global_tag"],
                               "create_by" : businput["create_by"],
                               "last_modification_date" : businput["last_modification_date"] ,
                               "last_modified_by" : businput["last_modified_by"]})

            # See if Processing Era exists
            if businput.has_key("processing_version"):
                dsdaoinput["processing_era_id"] = self.proceraid.execute(conn, businput["processing_version"], tran)
		if dsdaoinput["processing_era_id"] == -1 : raise Exception ("Processing Era : %s not found" %businput["processing_version"] )
            # See if Acquisition Era exists
            if businput.has_key("acquisition_era_name"):
                dsdaoinput["acquisition_era_id"] = self.acqeraid.execute(conn, businput["acquisition_era_name"], tran)
		if dsdaoinput["acquisition_era_id"] == -1 : raise Exception ("Acquisition Era : %s not found" %dsdaoinput["acquisition_era_id"] )
                 
            try:
                # insert the dataset
                self.datasetin.execute(conn, dsdaoinput, tran)
            except Exception, ex:
                if str(ex).lower().find("unique constraint") != -1 or str(ex).lower().find("duplicate") != -1:
                    # dataset already exists, lets fetch the ID
                    self.logger.warning("Unique constraint violation being ignored...")
                    self.logger.warning("%s" % ex)
		    ds = "/%s/%s/%s" %(businput["primary_ds_name"], businput["processed_ds_name"], businput["data_tier_name"])
                    dsdaoinput["dataset_id"] = self.datasetid.execute(conn, ds , tran)
                    if dsdaoinput["dataset_id"] == -1 : raise ("Strange error, the dataset %s does not exist ?" % ds)
                else:
                    raise	

            #FIXME : What about the READ-only status of the dataset
            # Create dataset_output_mod_mod_configs mapping
            if businput.has_key("output_configs"):
                for anOutConfig in businput["output_configs"]:
                    dsoutconfdaoin={}
                    dsoutconfdaoin["dataset_id"]=dsdaoinput["dataset_id"]
                    dsoutconfdaoin["output_mod_config_id"] = self.outconfigid.execute(conn, anOutConfig["app_name"], \
										anOutConfig["release_version"], \
										anOutConfig["pset_hash"], \
										anOutConfig["output_module_label"], tran) 
		    if dsoutconfdaoin["output_mod_config_id"] == -1 : raise Exception ("Output config (%s, %s, %s, %s) not found" %( anOutConfig["app_name"], \
                                                                                                                              anOutConfig["release_version"], \
                                                                                                                              anOutConfig["pset_hash"], \
                                                                                                                            anOutConfig["output_module_label"] ))
		    dsoutconfdaoin["ds_output_mod_conf_id"] = self.sm.increment(conn, "SEQ_DC", tran)
		    #print "INSERTING output_mod_config_id :::::: %s" %str(dsoutconfdaoin["output_mod_config_id"])
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
        except Exception, e:
            tran.rollback()
            self.logger.exception(e)
            raise
        finally:
            conn.close()
