#!/usr/bin/env python
"""
This module provides Dataset.List data access object.
"""
__revision__ = "$Id: List.py,v 1.3 2009/10/27 17:24:48 akhukhun Exp $"
__version__ = "$Revision: 1.3 $"

def op(pattern):
    """ returns 'like' if pattern includes '%' and '=' otherwise"""
    if pattern.find("%") == -1:
        return '='
    else:
        return 'like'

from WMCore.Database.DBFormatter import DBFormatter
class List(DBFormatter):
    """
    PrimaryDataset List DAO class.
    """
    def __init__(self, logger, dbi):
        """
        Add schema owner and sql.
        """
        DBFormatter.__init__(self, logger, dbi)
        self.owner = "%s." % self.dbi.engine.url.username
        self.sql = \
"""
SELECT D.DATASET_ID, D.DATASET, D.IS_DATASET_VALID, 
        D.PRIMARY_DS_ID,  PR.PRIMARY_DS_NAME, 
        D.PROCESSED_DS_ID, PS.PROCESSED_DS_NAME,
        D.DATA_TIER_ID, DT.DATA_TIER_NAME,
        D.PHYSICS_GROUP_ID, PH.PHYSICS_GROUP_NAME, 
        D.DATASET_TYPE_ID, DP.DATASET_TYPE,  
        D.ACQUISITION_ERA_ID, AE.ACQUISITION_ERA_NAME,
        D.PROCESSING_ERA_ID, PE.PROCESSING_VERSION,
        D.XTCROSSSECTION, 
        D.GLOBAL_TAG, D.CREATION_DATE, D.CREATE_BY, 
        D.LAST_MODIFICATION_DATE,D.LAST_MODIFIED_BY
	
FROM %sDATASETS D 
JOIN %sPRIMARY_DATASETS PR ON  PR.PRIMARY_DS_ID = D.PRIMARY_DS_ID 
JOIN %sPROCESSED_DATASETS PS ON PS.PROCESSED_DS_ID = D.PROCESSED_DS_ID  
JOIN %sDATA_TIERS DT ON DT.DATA_TIER_ID = D.DATA_TIER_ID
JOIN %sPHYSICS_GROUPS PH ON PH.PHYSICS_GROUP_ID = D.PHYSICS_GROUP_ID
JOIN %sDATASET_TYPES DP on DP.DATASET_TYPE_ID = D.DATASET_TYPE_ID
JOIN %sACQUISITION_ERAS AE on AE.ACQUISITION_ERA_ID = D.ACQUISITION_ERA_ID
JOIN %sPROCESSING_ERAS PE on PE.PROCESSING_ERA_ID = D.PROCESSING_ERA_ID
""" % ( self.owner, self.owner, self.owner, self.owner, self.owner, self.owner, self.owner, self.owner)
        
	
    def execute(self, primdsname = "", procdsname = "", datatiername = "", \
                conn = None, transaction = False):
        """
        Lists all datasets if none of the parameters are passed.
        Each parameter can include %.
        """	
        sql = self.sql
        if primdsname == "" and procdsname == "" and datatiername == "":
            result = self.dbi.processData(sql, conn=conn, transaction=transaction)
        else:
            binds = {}    
            if not primdsname == "":    
                sql += "WHERE PR.PRIMARY_DS_NAME %s :primarydsname" % op(primdsname)
                binds.update({"primarydsname":primdsname})
                
                if not procdsname == "":
                    sql += " AND PS.PROCESSED_DS_NAME %s :processeddsname" % op(procdsname)
                    binds.update({"processeddsname":procdsname})
                
                if not datatiername == "":
                    sql += " AND DT.DATA_TIER_NAME %s :datatiername" % op(datatiername)
                    binds.update({"datatiername":datatiername})
                    
            elif not procdsname == "":
                sql += " WHERE PS.PROCESSED_DS_NAME %s :processeddsname" % op(procdsname)
                binds.update({"processeddsname":procdsname})
                
                if not datatiername == "":
                    sql += " AND DT.DATA_TIER_NAME %s :datatiername" % op(datatiername)
                    binds.update({"datatiername":datatiername})
                    
            elif not datatiername == "":
                sql += " WHERE DT.DATA_TIER_NAME %s :datatiername" % op(datatiername)
                binds.update({"datatiername":datatiername})
                    
            result = self.dbi.processData(sql, binds, conn, transaction)
        ldict = self.formatDict(result)
        
        output = []
        
        for idict in ldict:
            primarydatasetdo = {"primary_ds_id":idict["primary_ds_id"],
                                "primary_ds_name":idict["primary_ds_name"]}
            processeddatasetdo = {"processed_ds_id":idict["processed_ds_id"],
                                  "processed_ds_name":idict["processed_ds_name"]}
            datatierdo = {"data_tier_id":idict["data_tier_id"],
                          "data_tier_name":idict["data_tier_name"]}
            physicsgroupdo = {"physics_group_id":idict["physics_group_id"],
                              "physics_group_name":idict["physics_group_id"]}
            datasettypedo = {"dataset_type_id":idict["dataset_type"],
                             "dataset_type":idict["dataset_type"]}
            acquisitionerado = {"acquisition_era_id":idict["acquisition_era_id"],
                                "acquisition_era_name":idict["acquisition_era_name"]}
            processingerado = {"processing_era_id":idict["processing_era_id"],
                               "processing_version":idict["processing_version"]}
            
            idict.update({"primary_ds_do":primarydatasetdo, 
                               "processed_ds_do":processeddatasetdo,
                               "data_tier_do":datatierdo,
                               "physics_group_do":physicsgroupdo,
                               "dataset_type_do":datasettypedo,
                               "acquisition_era_do":acquisitionerado,
                               "processing_era_do":processingerado})
            
            idict.pop("primary_ds_id") 
            idict.pop("primary_ds_name") 
            idict.pop("processed_ds_id") 
            idict.pop("processed_ds_name") 
            idict.pop("data_tier_id") 
            idict.pop("data_tier_name") 
            idict.pop("physics_group_id") 
            idict.pop("physics_group_name") 
            idict.pop("dataset_type_id") 
            idict.pop("dataset_type") 
            idict.pop("acquisition_era_id") 
            idict.pop("acquisition_era_name")
            idict.pop("processing_era_id")
            idict.pop("processing_version")
            output.append(idict)
           
        return output 

