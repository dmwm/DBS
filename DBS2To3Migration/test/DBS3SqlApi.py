"""
DBS3 SQL API for unittests to validate the DBS2 to DBS3 migration
Author: Manuel Giffels <giffels@physik.rwth-aachen.de>
"""

from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.DBFormatter import DBFormatter
from WMCore.DAOFactory import DAOFactory

class DBS3SqlApi(object):
    def __init__(self,logger,connectUrl,ownerDBS3,ownerDBS2):
        object.__init__(self)
        dbFactory = DBFactory(logger, connectUrl, options={})
        self.dbi = dbFactory.connect()

        daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=self.dbi, owner=ownerDBS3)

        self.dbFormatter = DBFormatter(logger,self.dbi)

        self.datasetListDAO = daofactory(classname="Dataset.List")
        self.dataTierListDAO = daofactory(classname="DataTier.List")
        self.primaryDatasetListDAO = daofactory(classname="PrimaryDataset.List")
        self.blockListDAO = daofactory(classname="Block.List")
        self.fileListDAO = daofactory(classname="File.List")
        self.outputModuleConfigDAO = daofactory(classname="OutputModuleConfig.List")
                
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
                              'FileLumis':'file_lumi_id',
                              'FileOutputModConfigs':'file_output_config_id',
                              'FileParents':'this_file_id',
                              'OutputModule':'output_mod_config_id',
                              'ParametersetHashes':'parameter_set_hash_id',
                              'PhysicsGroups':'physics_group_id',
                              'PrimaryDS':'primary_ds_id',
                              'PrimaryDSTypes':'primary_ds_type_id',
                              'ProcessedDatasets':'processed_ds_name',
                              'ReleaseVersions':'release_version_id'}

        self.sqlDict = {'AcquisitionEras':
                        """SELECT DISTINCT AE.ACQUISITION_ERA_NAME,
                        AE.CREATION_DATE,
                        AE.CREATE_BY,
                        AE.DESCRIPTION
                        FROM %s.ACQUISITION_ERAS AE
                        ORDER BY AE.ACQUISITION_ERA_NAME
                        """ % (ownerDBS3),
                        ##############################################
                        'ApplicationExecutables':
                        """SELECT AE.APP_EXEC_ID,
                        AE.APP_NAME
                        FROM %s.APPLICATION_EXECUTABLES AE
                        ORDER BY AE.APP_EXEC_ID
                        """ % (ownerDBS3),
                        ##############################################
                        'BlockParents':
                        """SELECT THIS_BLOCK_ID,
                        PARENT_BLOCK_ID
                        FROM (
                        SELECT BP.THIS_BLOCK_ID,
                        BP.PARENT_BLOCK_ID
                        FROM %s.BLOCK_PARENTS BP
                        UNION ALL
                        SELECT THISBLOCK this_block_id,
                        ITSPARENT parent_block_id
                        FROM %s.BLOCKPARENT)
                        GROUP BY THIS_BLOCK_ID,PARENT_BLOCK_ID
                        HAVING COUNT(*) = 1
                        ORDER BY THIS_BLOCK_ID, PARENT_BLOCK_ID
                        """ % (ownerDBS3,ownerDBS2),
                        ##############################################
                        'Dataset':
                        """
                        SELECT D.DATASET_ID, D.DATASET,
                        D.IS_DATASET_VALID, D.XTCROSSSECTION,
                        D.CREATION_DATE, D.CREATE_BY,
                        D.LAST_MODIFICATION_DATE, D.LAST_MODIFIED_BY,
                        P.PRIMARY_DS_NAME,
                        PDT.PRIMARY_DS_TYPE,
                        PD.PROCESSED_DS_NAME,
                        DT.DATA_TIER_NAME,
                        DP.DATASET_ACCESS_TYPE,
                        AE.ACQUISITION_ERA_NAME,
                        PE.PROCESSING_VERSION,
                        PH.PHYSICS_GROUP_NAME
                        FROM %s.DATASETS D
                        JOIN %s.PRIMARY_DATASETS P ON P.PRIMARY_DS_ID = D.PRIMARY_DS_ID
                        JOIN %s.PRIMARY_DS_TYPES PDT ON PDT.PRIMARY_DS_TYPE_ID = P.PRIMARY_DS_TYPE_ID
                        JOIN %s.PROCESSED_DATASETS PD ON PD.PROCESSED_DS_ID = D.PROCESSED_DS_ID
                        JOIN %s.DATA_TIERS DT ON DT.DATA_TIER_ID = D.DATA_TIER_ID
                        JOIN %s.DATASET_ACCESS_TYPES DP on DP.DATASET_ACCESS_TYPE_ID= D.DATASET_ACCESS_TYPE_ID
                        LEFT OUTER JOIN %s.ACQUISITION_ERAS AE ON AE.ACQUISITION_ERA_ID = D.ACQUISITION_ERA_ID
                        LEFT OUTER JOIN %s.PROCESSING_ERAS PE ON PE.PROCESSING_ERA_ID = D.PROCESSING_ERA_ID
                        LEFT OUTER JOIN %s.PHYSICS_GROUPS PH ON PH.PHYSICS_GROUP_ID = D.PHYSICS_GROUP_ID
                        ORDER BY DATASET_ID
                        """ % ((ownerDBS3,)*9),
                        ##############################################
                        ## Some datatypes are not existing in DBS2
                        'DatasetAccessTypes':
                        """SELECT DAT.DATASET_ACCESS_TYPE_ID,
                        DAT.DATASET_ACCESS_TYPE
                        FROM %s.DATASET_ACCESS_TYPES DAT
                        WHERE DAT.DATASET_ACCESS_TYPE_ID!=100
                        ORDER BY DATASET_ACCESS_TYPE_ID
                        """ % (ownerDBS3),
                        ##############################################
                        'DatasetOutputModConfigs':
                        """SELECT DOMC.DS_OUTPUT_MOD_CONF_ID,
                        DOMC.DATASET_ID,
                        DOMC.OUTPUT_MOD_CONFIG_ID
                        FROM %s.DATASET_OUTPUT_MOD_CONFIGS DOMC
                        ORDER BY DOMC.DS_OUTPUT_MOD_CONF_ID
                        """ % (ownerDBS3),
                        ##############################################
                        'DatasetParents':
                        """SELECT DP.THIS_DATASET_ID,
                        DP.PARENT_DATASET_ID
                        FROM %s.DATASET_PARENTS DP
                        ORDER BY DP.THIS_DATASET_ID, DP.PARENT_DATASET_ID
                        """ % (ownerDBS3),
                        ##############################################
                        'DatasetRuns':
                        """SELECT DR.DATASET_RUN_ID,
                        DR.DATASET_ID,
                        DR.RUN_NUMBER,
                        DR.COMPLETE,
                        DR.LUMI_SECTION_COUNT,
                        DR.CREATION_DATE,
                        DR.CREATE_BY
                        FROM %s.DATASET_RUNS
                        ORDER BY DR.DATASET_RUN_ID
                        """ % (ownerDBS3),
                        ##############################################
                        'Files':
                        """
                        SELECT FILE_ID, LOGICAL_FILE_NAME, IS_FILE_VALID,
                        DATASET_ID, DATASET,
                        BLOCK_ID, BLOCK_NAME,
                        FILE_TYPE_ID, FILE_TYPE,
                        CHECK_SUM, EVENT_COUNT, FILE_SIZE, 
                        BRANCH_HASH_ID, ADLER32, MD5,
                        AUTO_CROSS_SECTION,
                        CREATION_DATE, CREATE_BY,
                        LAST_MODIFICATION_DATE, LAST_MODIFIED_BY
                        FROM
                        (
                        SELECT F.FILE_ID, F.LOGICAL_FILE_NAME, F.IS_FILE_VALID,
                        F.DATASET_ID, D.DATASET,
                        F.BLOCK_ID, B.BLOCK_NAME,
                        F.FILE_TYPE_ID, FT.FILE_TYPE,
                        F.CHECK_SUM, F.EVENT_COUNT, F.FILE_SIZE, 
                        F.BRANCH_HASH_ID, F.ADLER32, F.MD5,
                        F.AUTO_CROSS_SECTION,
                        F.CREATION_DATE, F.CREATE_BY,
                        F.LAST_MODIFICATION_DATE, F.LAST_MODIFIED_BY
                        FROM %s.FILES F
                        JOIN %s.FILE_DATA_TYPES FT ON  FT.FILE_TYPE_ID = F.FILE_TYPE_ID
                        JOIN %s.DATASETS D ON  D.DATASET_ID = F.DATASET_ID
                        JOIN %s.BLOCKS B ON B.BLOCK_ID = F.BLOCK_ID
                        UNION ALL
                        SELECT
                        FS2.ID file_id,
                        FS2.LOGICALFILENAME logical_file_name,
                        FS2.VALIDATIONSTATUS is_file_valid,
                        FS2.DATASET dataset_id,
                        '/' || PD2.NAME || '/' || DS2.NAME || '/' || DT2.NAME dataset,
                        FS2.BLOCK block_id,
                        BL2.NAME block_name,
                        FS2.FILETYPE file_type_id,
                        FT2.TYPE file_type,
                        FS2.CHECKSUM check_sum,
                        FS2.NUMBEROFEVENTS event_count,
                        FS2.FILESIZE file_size,
                        FS2.FILEBRANCH branch_hash_id,
                        FS2.ADLER32,
                        FS2.MD5,
                        FS2.AUTOCROSSSECTION auto_cross_section,
                        FS2.CREATIONDATE creation_date,
                        PS12.DISTINGUISHEDNAME create_by,
                        FS2.LASTMODIFICATIONDATE last_modification_date,
                        PS22.DISTINGUISHEDNAME last_modified_by
                        FROM %s.FILES FS2
                        JOIN %s.PROCESSEDDATASET DS2 ON DS2.ID=FS2.DATASET
                        JOIN %s.PRIMARYDATASET PD2 on DS2.PRIMARYDATASET=PD2.ID
                        JOIN %s.DATATIER DT2 ON DS2.DATATIER=DT2.ID
                        JOIN %s.PERSON PS12 ON FS2.CREATEDBY=PS12.ID
                        JOIN %s.PERSON PS22 ON FS2.LASTMODIFIEDBY=PS22.ID
                        JOIN %s.BLOCK BL2 ON FS2.BLOCK=BL2.ID
                        JOIN %s.FILETYPE FT2 ON FT2.ID=FS2.FILETYPE
                        )
                        GROUP BY FILE_ID, LOGICAL_FILE_NAME, IS_FILE_VALID,
                        DATASET_ID, DATASET,
                        BLOCK_ID, BLOCK_NAME,
                        FILE_TYPE_ID, FILE_TYPE,
                        CHECK_SUM, EVENT_COUNT, FILE_SIZE, 
                        BRANCH_HASH_ID, ADLER32, MD5,
                        AUTO_CROSS_SECTION,
                        CREATION_DATE, CREATE_BY,
                        LAST_MODIFICATION_DATE, LAST_MODIFIED_BY
                        HAVING COUNT(*) = 1
                        ORDER BY FILE_ID
                        """ % (ownerDBS3,ownerDBS3,ownerDBS3,ownerDBS3,ownerDBS2,ownerDBS2,ownerDBS2,ownerDBS2,ownerDBS2,ownerDBS2,ownerDBS2,ownerDBS2),
                        ##############################################
                        'FileDataTypes':
                        """SELECT FDT.FILE_TYPE_ID,
                        FDT.FILE_TYPE
                        FROM %s.FILE_DATA_TYPES FDT
                        ORDER BY FILE_TYPE_ID
                        """ % (ownerDBS3),
                        ##############################################
                        'FileLumis':
                        """SELECT FILE_LUMI_ID,RUN_NUM,LUMI_SECTION_NUM,FILE_ID FROM
                        (SELECT FL.FILE_LUMI_ID,FL.RUN_NUM,FL.LUMI_SECTION_NUM,FL.FILE_ID
                        FROM %s.FILE_LUMIS FL
                        UNION ALL
                        SELECT FRL.ID file_lumi_id, FRL.RUN run_num, FRL.LUMI lumi_section_num, FRL.FILEID file_id
                        FROM %s.FILERUNLUMI FRL
                        )
                        GROUP BY FILE_LUMI_ID,RUN_NUM,LUMI_SECTION_NUM,FILE_ID
                        HAVING COUNT(*) = 1
                        ORDER BY FILE_LUMI_ID
                        """ % (ownerDBS3,ownerDBS2),
                        ##############################################
                        'FileOutputModConfigs':
                        """SELECT FILE_OUTPUT_CONFIG_ID,FILE_ID,OUTPUT_MOD_CONFIG_ID FROM
                        (SELECT FOMC.FILE_OUTPUT_CONFIG_ID,FOMC.FILE_ID,FOMC.OUTPUT_MOD_CONFIG_ID
                        FROM %s.FILE_OUTPUT_MOD_CONFIGS FOMC
                        UNION ALL
                        SELECT FA.ID file_output_config_id, FA.FILEID file_id, FA.ALGORITHM output_mod_config_id
                        FROM %s.FILEALGO FA
                        )
                        GROUP BY FILE_OUTPUT_CONFIG_ID,FILE_ID,OUTPUT_MOD_CONFIG_ID
                        HAVING COUNT(*) = 1
                        ORDER BY FILE_OUTPUT_CONFIG_ID
                        """ % (ownerDBS3,ownerDBS2),
                        ##############################################
                        'FileParents':
                        """SELECT THIS_FILE_ID,PARENT_FILE_ID FROM
                        (SELECT FP.THIS_FILE_ID,FP.PARENT_FILE_ID
                        FROM %s.FILE_PARENTS FP
                        UNION ALL
                        SELECT FP2.THISFILE this_file_id, FP2.ITSPARENT parent_file_id
                        FROM %s.FILEPARENTAGE FP2)
                        GROUP BY THIS_FILE_ID,PARENT_FILE_ID
                        HAVING COUNT(*) = 1
                        ORDER BY THIS_FILE_ID,PARENT_FILE_ID
                        """ % (ownerDBS3,ownerDBS2),
                        ##############################################
                        'OutputModule':
                        """SELECT O.OUTPUT_MOD_CONFIG_ID,
                        AE.APP_NAME,
                        RV.RELEASE_VERSION,
                        PSH.PARAMETER_SET_HASH_ID,
                        PSH.PSET_HASH,
                        PSH.NAME pset_name,
                        O.OUTPUT_MODULE_LABEL,
                        O.GLOBAL_TAG,
                        O.SCENARIO,
                        O.CREATION_DATE,
                        O.CREATE_BY
                        FROM %s.OUTPUT_MODULE_CONFIGS O
                        JOIN %s.APPLICATION_EXECUTABLES AE ON O.APP_EXEC_ID=AE.APP_EXEC_ID
                        JOIN %s.RELEASE_VERSIONS RV ON O.RELEASE_VERSION_ID=RV.RELEASE_VERSION_ID
                        JOIN %s.PARAMETER_SET_HASHES PSH ON O.PARAMETER_SET_HASH_ID=PSH.PARAMETER_SET_HASH_ID
                        ORDER BY OUTPUT_MOD_CONFIG_ID
                        """ % (ownerDBS3,ownerDBS3,ownerDBS3,ownerDBS3),
                        ##############################################
                        'ParametersetHashes':
                        """SELECT PH.PARAMETER_SET_HASH_ID,
                        PH.PSET_HASH,
                        PH.NAME
                        FROM %s.PARAMETER_SET_HASHES PH
                        ORDER BY PARAMETER_SET_HASH_ID
                        """ % (ownerDBS3),
                        ##############################################
                        'PhysicsGroups':
                        """SELECT PG.PHYSICS_GROUP_ID,
                        PG.PHYSICS_GROUP_NAME
                        FROM %s.PHYSICS_GROUPS PG
                        ORDER BY PHYSICS_GROUP_ID
                        """ % (ownerDBS3),
                        ##############################################
                        'PrimaryDSTypes':
                        """SELECT PDST.PRIMARY_DS_TYPE_ID,
                        PDST.PRIMARY_DS_TYPE
                        FROM %s.PRIMARY_DS_TYPES PDST
                        """ % (ownerDBS3),
                        ##############################################
                        'ProcessedDatasets':
                        """SELECT DISTINCT PCD.PROCESSED_DS_NAME
                        FROM %s.PROCESSED_DATASETS PCD
                        ORDER BY PCD.PROCESSED_DS_NAME
                        """ % (ownerDBS3),
                        ##############################################
                        'ReleaseVersions':
                        """SELECT RV.RELEASE_VERSION_ID,
                        RV.RELEASE_VERSION
                        FROM %s.RELEASE_VERSIONS RV
                        ORDER BY RELEASE_VERSION_ID
                        """ % (ownerDBS3)
                        }

    def acquisitionEras(self,sort=True):
        return self._queryDB('AcquisitionEras',sort=sort)

    def applicationExecutables(self,sort=True):
        return self._queryDB('ApplicationExecutables',sort=sort)
        
    def blockList(self,sort=True):
        result = self.blockListDAO.execute(self.dbi.connection())
        return sorted(result,key=lambda entry: entry[self.sqlPrimaryKey['Block']])

    def blockParents(self,sort=True):
        return self._queryDB('BlockParents',sort=sort)

    def datasetList(self,sort=True):
        return self._queryDB('Dataset',sort=sort)

    def datasetAccessTypes(self,sort=True):
        return self._queryDB('DatasetAccessTypes',sort=sort)

    def datasetOutputModConfigs(self,sort=True):
        return self._queryDB('DatasetOutputModConfigs',sort=sort)
    
    def datasetParents(self,sort=True):
        return self._queryDB('DatasetParents',sort=sort)

    def datasetRuns(self,sort=True):
        return self._queryDB('DatasetRuns',sort=sort)

    def dataTierList(self,sort=True):
        result = self.dataTierListDAO.execute(self.dbi.connection(),None)
        return sorted(result,key=lambda entry: entry[self.sqlPrimaryKey['DataTier']])

    def fileMinMaxCount(self,sort=False):
        return self._queryDB('FileMinMaxCount',sort=sort)

    def fileDataTypes(self,sort=True):
        return self._queryDB('FileDataTypes',sort=sort)

    def fileList(self,sort=True):
        return self._queryDB('Files',sort=sort)

    def fileLumis(self,sort=True):
        return self._queryDB('FileLumis',sort=sort)

    def fileOutputModConfigs(self,sort=True):
        return self._queryDB('FileOutputModConfigs',sort=sort)

    def fileParents(self,sort=True):
        return self._queryDB('FileParents',sort=sort)

    def parametersetHashes(self,sort=True):
        return self._queryDB('ParametersetHashes',sort=sort)

    def outputModuleConfig(self,sort=True):
        return self._queryDB('OutputModule',sort=sort)

    def physicsGroups(self,sort=True):
        return self._queryDB('PhysicsGroups',sort=sort)

    def primaryDatasetList(self,sort=True):
        result = self.primaryDatasetListDAO.execute(self.dbi.connection())
        return sorted(result,key=lambda entry: entry[self.sqlPrimaryKey['PrimaryDS']])

    def primaryDSTypes(self,sort=True):
        return self._queryDB('PrimaryDSTypes')
        
    def processedDatasets(self,sort=True):
        return self._queryDB('ProcessedDatasets')
        
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


