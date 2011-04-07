"""
DBS2 SQL API for unittests to validate the DBS2 to DBS3 migration
Author: Manuel Giffels <giffels@physik.rwth-aachen.de>
"""

from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.DBFormatter import DBFormatter

def modifyDatasetList(datasetList):
    ### DBS3 has additional fields
    for dataset in datasetList:
        dataset['dataset'] = '/'+dataset['primary_ds_name']+'/'+dataset['processed_ds_name']+'/'+dataset['data_tier_name']
        dataset['is_dataset_valid'] = int(dataset['status'] in ('VALID', 'RO', 'PRODUCTION', 'IMPORTED', 'EXPORTED'))

        if dataset['status'] in ('VALID','INVALID'):
            dataset['dataset_access_type'] = 'UNKNOWN_DBS2_TYPE'
        else:
            dataset['dataset_access_type'] = dataset['status']
            
        dataset['processing_version'] = None
        del dataset['status']
        
    return datasetList

class DBS2SqlApi(object):
    def __init__(self,logger,connectUrl,owner):
        object.__init__(self)
        dbFactory = DBFactory(logger, connectUrl, options={})
        self.dbi = dbFactory.connect()
        self.dbFormatter = DBFormatter(logger,self.dbi)

        self.sqlDict = {'AcquisitionEras':
                        """SELECT DISTINCT PCD.AQUISITIONERA acquisition_era_name,
                        NULL description,
                        NULL create_by,
                        NULL creation_date
                        FROM %s.PROCESSEDDATASET PCD
                        WHERE AQUISITIONERA IS NOT NULL
                        ORDER BY PCD.AQUISITIONERA
                        """ % (owner),
                        ##############################################
                        'ApplicationExecutables':
                        """SELECT AE.ID app_exec_id,
                        AE.EXECUTABLENAME app_name
                        FROM %s.APPEXECUTABLE AE
                        ORDER BY app_exec_id
                        """ % (owner),
                        ##############################################
                        'Block':
                        """SELECT BL.ID block_id,
                        BL.NAME block_name,
                        BL.PATH dataset,
                        BL.BLOCKSIZE block_size,
                        BL.NUMBEROFFILES file_count,
                        BL.OPENFORWRITING open_for_writing,
                        PS1.DISTINGUISHEDNAME CREATE_BY,
                        PS2.DISTINGUISHEDNAME last_modified_by,
                        BL.LASTMODIFICATIONDATE last_modification_date ,
                        BL.CREATIONDATE creation_date,
                        BL.DATASET dataset_id, '/' || PD.NAME || '/' || DS.NAME || '/' || DT.NAME dataset,
                        'UNKNOWN' origin_site_name
                        FROM %s.BLOCK BL
                        JOIN %s.PERSON PS1 ON BL.CREATEDBY=PS1.ID
                        JOIN %s.PERSON PS2 ON BL.LASTMODIFIEDBY=PS2.ID
                        JOIN %s.PROCESSEDDATASET DS ON DS.ID=BL.DATASET
                        JOIN %s.PRIMARYDATASET PD on DS.PRIMARYDATASET=PD.ID
                        JOIN %s.DATATIER DT ON DS.DATATIER=DT.ID
                        ORDER BY block_id
                        """ % (owner,owner,owner,owner,owner,owner),
                        ##############################################
                        'BlockParents':
                        """SELECT BP.THISBLOCK this_block_id,
                        BP.ITSPARENT parent_block_id
                        FROM %s.BLOCKPARENT BP
                        ORDER BY BP.THISBLOCK,BP.ITSPARENT
                        """ % (owner),
                        ##############################################
                        'Dataset':
                        """SELECT DS.ID dataset_id,
                        DS.NAME processed_ds_name,
                        PD.NAME primary_ds_name,
                        DT.NAME data_tier_name,
                        PG.PHYSICSGROUPNAME physics_group_name,
                        ST.STATUS,
                        DS.AQUISITIONERA acquisition_era_name,
                        DS.XTCROSSSECTION,
                        PS1.DISTINGUISHEDNAME CREATE_BY,
                        DS.CREATIONDATE creation_date,
                        PS2.DISTINGUISHEDNAME LAST_MODIFIED_BY,
                        DS.LASTMODIFICATIONDATE last_modification_date,
                        PT.TYPE primary_ds_type
                        FROM %s.PROCESSEDDATASET DS
                        JOIN %s.DATATIER DT ON DS.DATATIER=DT.ID
                        JOIN %s.PRIMARYDATASET PD ON PD.ID=DS.PRIMARYDATASET
                        JOIN %s.PHYSICSGROUP PG ON PG.ID=DS.PHYSICSGROUP
                        JOIN %s.PROCDSSTATUS ST ON ST.ID=DS.STATUS
                        JOIN %s.PERSON PS1 ON DS.CREATEDBY=PS1.ID
                        JOIN %s.PERSON PS2 ON DS.LASTMODIFIEDBY=PS2.ID
                        JOIN %s.PRIMARYDSTYPE PT ON PT.ID=PD.TYPE
                        ORDER BY dataset_id
                        """ % (owner,owner,owner,owner,owner,owner,owner,owner),
                        ##############################################
                        ## Some datatypes are not existing anymore in DBS3
                        'DatasetAccessTypes':
                        """SELECT PDS.ID dataset_access_type_id,
                        PDS.STATUS dataset_access_type
                        FROM %s.PROCDSSTATUS PDS
                        WHERE PDS.ID!=2 AND PDS.ID!=61
                        ORDER BY dataset_access_type_id
                        """ % (owner),
                        ##############################################
                        'DatasetOutputModConfigs':
                        """SELECT PA.ID ds_output_mod_conf_id,
                        PA.DATASET dataset_id,
                        PA.ALGORITHM output_mod_config_id
                        FROM %s.PROCALGO PA
                        ORDER BY ds_output_mod_conf_id
                        """ % (owner),
                        ##############################################
                        'DatasetParents':
                        """SELECT DP.THISDATASET this_dataset_id,
                        DP.ITSPARENT parent_dataset_id
                        FROM %s.PROCDSPARENT DP
                        ORDER BY this_dataset_id,parent_dataset_id
                        """ % (owner),
                        ##############################################
                        'DataTier':
                        """SELECT DT.ID data_tier_id,
                        DT.NAME data_tier_name,
                        PS.DISTINGUISHEDNAME create_by,
                        DT.CREATIONDATE creation_date
                        FROM %s.DATATIER DT
                        JOIN %s.PERSON PS ON PS.ID=DT.CREATEDBY
                        ORDER BY data_tier_id
                        """ % (owner,owner),
                        ##############################################
                        'FileDataTypes':
                        """SELECT FDT.ID file_type_id,
                        FDT.TYPE file_type
                        FROM %s.FILETYPE FDT
                        ORDER BY file_type_id
                        """ % (owner),
                        ##############################################
                        'OutputModule':
                        """
                        SELECT AC.ID output_mod_config_id,
                        APPEX.EXECUTABLENAME app_name,
                        APPVER.VERSION release_version,
                        AC.PARAMETERSETID parameter_set_hash_id,
                        TO_CHAR(AC.APPLICATIONFAMILY) output_module_label,
                        'UNKNOWN' global_tag,
                        NULL scenario,
                        QPS.HASH pset_hash,
                        QPS.NAME pset_name,
                        AC.CREATIONDATE creation_date,
                        PS.DISTINGUISHEDNAME create_by
                        FROM %s.ALGORITHMCONFIG AC
                        JOIN %s.APPEXECUTABLE APPEX ON APPEX.ID=AC.EXECUTABLENAME
                        JOIN %s.APPVERSION APPVER ON APPVER.ID=AC.APPLICATIONVERSION
                        JOIN %s.PERSON PS ON PS.ID=AC.CREATEDBY
                        JOIN %s.QUERYABLEPARAMETERSET QPS ON QPS.ID=AC.PARAMETERSETID
                        ORDER BY output_mod_config_id
                        """ % (owner,owner,owner,owner,owner),
                        ##############################################
                        'ParametersetHashes':
                        """SELECT QP.ID parameter_set_hash_id,
                        QP.HASH pset_hash,
                        QP.NAME
                        FROM %s.QUERYABLEPARAMETERSET QP
                        ORDER BY parameter_set_hash_id
                        """ % (owner),
                        ##############################################
                        'PhysicsGroups':
                        """SELECT PG.ID physics_group_id,
                        PG.PHYSICSGROUPNAME physics_group_name
                        FROM %s.PHYSICSGROUP PG
                        ORDER BY physics_group_id
                        """ % (owner),
                        ##############################################
                        'PrimaryDS':
                        """SELECT PD.ID primary_ds_id,
                        PD.NAME primary_ds_name,
                        PT.TYPE primary_ds_type,
                        PD.CREATIONDATE creation_date,
                        PS.DISTINGUISHEDNAME create_by
                        FROM %s.PRIMARYDATASET PD
                        JOIN %s.PERSON PS ON PS.ID=PD.CREATEDBY
                        JOIN %s.PRIMARYDSTYPE PT ON PT.ID=PD.TYPE
                        ORDER BY primary_ds_id
                        """ % (owner,owner,owner),
                        ##############################################
                        'PrimaryDSTypes':
                        """SELECT PDST.ID primary_ds_type_id,
                        PDST.TYPE primary_ds_type
                        FROM %s.PRIMARYDSTYPE PDST
                        ORDER BY primary_ds_type_id
                        """ % (owner),
                        ##############################################
                        'ProcessedDatasets':
                        """SELECT DISTINCT PCD.NAME processed_ds_name
                        FROM %s.PROCESSEDDATASET PCD
                        ORDER BY PCD.NAME
                        """ % (owner),
                        ##############################################
                        'ReleaseVersions':
                        """SELECT RV.ID release_version_id,
                        RV.VERSION release_version
                        FROM %s.APPVERSION RV
                        ORDER BY release_version_id
                        """ % (owner)
                        }
                                
        self.sqlPrimaryKey = {'AcquisitionEras':'acquisition_era_name',
                              'ApplicationExecutables':'app_exec_id',
                              'Block':'block_id',
                              'BlockParents':'this_block_id',
                              'Dataset':'dataset_id',
                              'DatasetAccessTypes':'dataset_access_type_id',
                              'DatasetOutputModConfigs':'ds_output_mod_conf_id',
                              'DatasetParents':'this_dataset_id',
                              'DatasetRuns':'dataset_run_id',
                              'DataTier':'data_tier_id',
                              'Files':'file_id',
                              'FileDataTypes':'file_type_id',
                              'OutputModule':'output_mod_config_id',
                              'ParametersetHashes':'parameter_set_hash_id',
                              'PhysicsGroups':'physics_group_id',
                              'PrimaryDS':'primary_ds_id',
                              'PrimaryDSTypes':'primary_ds_type_id',
                              'ProcessedDatasets':'processed_ds_name',
                              'ReleaseVersions':'release_version_id'}

    def acquisitionEras(self,sort=True):
        return self._queryDB('AcquisitionEras',sort=sort)

    def applicationExecutables(self,sort=True):
        return self._queryDB('ApplicationExecutables',sort=sort)

    def blockList(self,sort=True):
        return self._queryDB('Block',sort=sort)

    def blockParents(self,sort=True):
        return self._queryDB('BlockParents',sort=sort)

    def datasetList(self,sort=True):
        #ToDo mapping DBS2 PROCDSSTATUS to is_dataset_valid and dataset_access_type in DBS3
        #ToDo compare GlobalTAG when comparing output_module_configs
        return modifyDatasetList(self._queryDB('Dataset',sort=sort))

    def datasetAccessTypes(self,sort=True):
        return self._queryDB('DatasetAccessTypes',sort=sort)

    def datasetOutputModConfigs(self,sort=True):
        return self._queryDB('DatasetOutputModConfigs',sort=sort)
    
    def datasetParents(self,sort=True):
        return self._queryDB('DatasetParents',sort=sort)

    def datasetRuns(self,sort=True):
        return self._queryDB('DatasetRuns',sort=sort)
    
    def dataTierList(self,sort=True):
        return self._queryDB('DataTier',sort=sort)

    def fileDataTypes(self,sort=True):
        return self._queryDB('FileDataTypes',sort=sort)        

    def outputModuleConfig(self,sort=True):
        return self._queryDB('OutputModule',sort=sort)

    def parametersetHashes(self,sort=True):
        return self._queryDB('ParametersetHashes',sort=sort)

    def physicsGroups(self,sort=True):
        return self._queryDB('PhysicsGroups',sort=sort)
    
    def primaryDatasetList(self,sort=True):
        return self._queryDB('PrimaryDS',sort=sort)

    def primaryDSTypes(self,sort=True):
        return self._queryDB('PrimaryDSTypes',sort=sort)

    def processedDatasets(self,sort=True):
        return self._queryDB('ProcessedDatasets',sort=sort)

    def releaseVersions(self,sort=True):
        return self._queryDB('ReleaseVersions',sort=sort)

    def _queryDB(self,query,binds={},sort=True):
        connection = self.dbi.connection()

        cursors = self.dbi.processData(self.sqlDict[query],
                                      binds,
                                      connection,
                                      transaction=False,
                                      returnCursor=True)

        result = self.dbFormatter.formatCursor(cursors[0])

        connection.close()
        
        if sort:
            return sorted(result,key=lambda entry: entry[self.sqlPrimaryKey[query]])
        else:
            return result
