"""
DBS SQL QUERIES for unittests to validate the DBS2 to DBS3 migration
"""

from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.DBFormatter import DBFormatter

class DBSSqlQueries(object):
    def __init__(self, logger, connectUrl, ownerDBS3, ownerDBS2):
        object.__init__(self)
        dbFactory = DBFactory(logger, connectUrl, options={})
        self.dbi = dbFactory.connect()

        self.dbFormatter = DBFormatter(logger, self.dbi)

        self.sqlPrimaryKey = {'AcquisitionEras': 'acquisition_era_name',
                              'ApplicationExecutables': 'app_exec_id',
                              'Block': 'block_id',
                              'BlockParents': 'this_block_id',
                              'Dataset': 'dataset_id',
                              'DatasetAccessTypes': 'dataset_access_type_id',
                              'DatasetOutputModConfigs': 'ds_output_mod_conf_id',
                              'DatasetParents': 'this_dataset_id',
                              'DatasetRuns': 'dataset_run_id',
                              'DataTier': 'data_tier_id',
                              'Files': 'file_id',
                              'FileDataTypes': 'file_type_id',
                              'FileLumis': 'file_lumi_id',
                              'FileOutputModConfigs': 'file_output_config_id',
                              'FileParents': 'this_file_id',
                              'OriginSiteName': 'block_id',
                              'OutputModule': 'output_mod_config_id',
                              'ParametersetHashes': 'parameter_set_hash_id',
                              'PhysicsGroups': 'physics_group_id',
                              'PrimaryDS': 'primary_ds_id',
                              'PrimaryDSTypes': 'primary_ds_type_id',
                              'ProcessedDatasets': 'processed_ds_name',
                              'ReleaseVersions': 'release_version_id'}

        self.sqlDict = {'AcquisitionEras':
                        """SELECT ACQUISITION_ERA_NAME,
                        START_DATE,
                        END_DATE
                        CREATION_DATE,
                        CREATE_BY,
                        DESCRIPTION
                        FROM(
                        SELECT DISTINCT AE.ACQUISITION_ERA_NAME,
                        AE.START_DATE,
                        AE.END_DATE,
                        AE.CREATION_DATE,
                        AE.CREATE_BY,
                        AE.DESCRIPTION
                        FROM {ownerDBS3}.ACQUISITION_ERAS AE
                        UNION ALL
                        SELECT DISTINCT PCD.AQUISITIONERA ACQUISITION_ERA_NAME,
                        0 START_DATE,
                        NULL END_DATE,
                        NULL CREATION_DATE,
                        NULL CREATE_BY,
                        NULL DESCRIPTION
                        FROM {ownerDBS2}.PROCESSEDDATASET PCD
                        WHERE AQUISITIONERA IS NOT NULL
                        )
                        GROUP BY ACQUISITION_ERA_NAME,
                        START_DATE,
                        END_DATE,
                        CREATION_DATE,
                        CREATE_BY,
                        DESCRIPTION
                        HAVING COUNT(*) <> 2
                        ORDER BY ACQUISITION_ERA_NAME
                        """.format(ownerDBS3=ownerDBS3, ownerDBS2=ownerDBS2),
                        ##############################################
                        'ApplicationExecutables':
                        """SELECT APP_EXEC_ID,
                        APP_NAME
                        FROM(
                        SELECT AE.APP_EXEC_ID,
                        AE.APP_NAME
                        FROM {ownerDBS3}.APPLICATION_EXECUTABLES AE
                        UNION ALL
                        SELECT AE2.ID APP_EXEC_ID,
                        AE2.EXECUTABLENAME APP_NAME
                        FROM {ownerDBS2}.APPEXECUTABLE AE2
                        )
                        GROUP BY APP_EXEC_ID,
                        APP_NAME
                        HAVING COUNT(*) <> 2
                        ORDER BY APP_EXEC_ID
                        """.format(ownerDBS3=ownerDBS3, ownerDBS2=ownerDBS2),
                        ##############################################
                        'Block':
                        """SELECT BLOCK_ID,
                        BLOCK_NAME,
                        DATASET_ID,
                        PATH,
                        OPEN_FOR_WRITING,
                        ORIGIN_SITE_NAME,
                        BLOCK_SIZE,
                        FILE_COUNT,
                        CREATION_DATE,
                        CREATE_BY,
                        LAST_MODIFICATION_DATE,
                        LAST_MODIFIED_BY
                        FROM(
                        SELECT BL.BLOCK_ID,
                        BL.BLOCK_NAME,
                        BL.DATASET_ID,
                        DS.DATASET PATH, 
                        BL.OPEN_FOR_WRITING,
                        BL.ORIGIN_SITE_NAME,
                        BL.BLOCK_SIZE,
                        BL.FILE_COUNT,
                        BL.CREATION_DATE,
                        BL.CREATE_BY,
                        BL.LAST_MODIFICATION_DATE,
                        BL.LAST_MODIFIED_BY
                        FROM {ownerDBS3}.BLOCKS BL
                        JOIN {ownerDBS3}.DATASETS DS ON BL.DATASET_ID=DS.DATASET_ID
                        UNION ALL
                        SELECT BL2.ID BLOCK_ID,
                        BL2.NAME BLOCK_NAME,
                        BL2.DATASET DATASET_ID,
                        BL2.PATH,
                        BL2.OPENFORWRITING OPEN_FOR_WRITING,
                        'UNKNOWN' ORIGIN_SITE_NAME,
                        BL2.BLOCKSIZE BLOCK_SIZE,
                        BL2.NUMBEROFFILES FILE_COUNT,
                        BL2.CREATIONDATE CREATION_DATE,
                        PS1.DISTINGUISHEDNAME CREATE_BY,
                        BL2.LASTMODIFICATIONDATE LAST_MODIFICATION_DATE,
                        PS2.DISTINGUISHEDNAME LAST_MODIFIED_BY
                        FROM {ownerDBS2}.BLOCK BL2
                        JOIN {ownerDBS2}.PERSON PS1 ON BL2.CREATEDBY=PS1.ID
                        JOIN {ownerDBS2}.PERSON PS2 ON BL2.LASTMODIFIEDBY=PS2.ID
                        JOIN {ownerDBS2}.PROCESSEDDATASET DS ON DS.ID=BL2.DATASET
                        JOIN {ownerDBS2}.PRIMARYDATASET PD on DS.PRIMARYDATASET=PD.ID
                        JOIN {ownerDBS2}.DATATIER DT ON DS.DATATIER=DT.ID
                        )
                        GROUP BY 
                        BLOCK_ID,
                        BLOCK_NAME,
                        DATASET_ID,
                        PATH,
                        OPEN_FOR_WRITING,
                        ORIGIN_SITE_NAME,
                        BLOCK_SIZE,
                        FILE_COUNT,
                        CREATION_DATE,
                        CREATE_BY,
                        LAST_MODIFICATION_DATE,
                        LAST_MODIFIED_BY
                        HAVING COUNT(*) <> 2
                        ORDER BY BLOCK_ID
                        """.format(ownerDBS3=ownerDBS3, ownerDBS2=ownerDBS2),
                        ##############################################
                        'BlockParents':
                        """SELECT THIS_BLOCK_ID,
                        PARENT_BLOCK_ID
                        FROM (
                        SELECT BP.THIS_BLOCK_ID,
                        BP.PARENT_BLOCK_ID
                        FROM {ownerDBS3}.BLOCK_PARENTS BP
                        UNION ALL
                        SELECT THISBLOCK this_block_id,
                        ITSPARENT parent_block_id
                        FROM {ownerDBS2}.BLOCKPARENT)
                        GROUP BY THIS_BLOCK_ID,PARENT_BLOCK_ID
                        HAVING COUNT(*) <> 2
                        ORDER BY THIS_BLOCK_ID, PARENT_BLOCK_ID
                        """.format(ownerDBS3=ownerDBS3, ownerDBS2=ownerDBS2),
                        ##############################################
                        'DataTier':
                        """SELECT DATA_TIER_ID,
                        DATA_TIER_NAME,
                        CREATION_DATE,
                        CREATE_BY
                        FROM(
                        SELECT DT.DATA_TIER_ID,
                        DT.DATA_TIER_NAME,
                        DT.CREATION_DATE,
                        DT.CREATE_BY 
                        FROM {ownerDBS3}.DATA_TIERS DT
                        UNION ALL
                        SELECT DT.ID DATA_TIER_ID,
                        DT.NAME DATA_TIER_NAME,
                        DT.CREATIONDATE CREATION_DATE,
                        PS.DISTINGUISHEDNAME CREATE_BY
                        FROM {ownerDBS2}.DATATIER DT
                        JOIN {ownerDBS2}.PERSON PS ON PS.ID=DT.CREATEDBY
                        )
                        GROUP BY DATA_TIER_ID,
                        DATA_TIER_NAME,
                        CREATION_DATE,
                        CREATE_BY
                        HAVING COUNT(*) <> 2
                        ORDER BY data_tier_id
                        """.format(ownerDBS3=ownerDBS3, ownerDBS2=ownerDBS2),
                        ##############################################
                        'Dataset':
                        """SELECT DATASET_ID,
                        DATASET,
                        XTCROSSSECTION,
                        CREATION_DATE,
                        CREATE_BY,
                        LAST_MODIFICATION_DATE,
                        LAST_MODIFIED_BY,
                        PRIMARY_DS_NAME,
                        PRIMARY_DS_TYPE,
                        PROCESSED_DS_NAME,
                        DATA_TIER_NAME,
                        DATASET_ACCESS_TYPE,
                        ACQUISITION_ERA_NAME,
                        PROCESSING_ERA_ID,
                        PHYSICS_GROUP_NAME,
                        PREP_ID
                        FROM(
                        SELECT D.DATASET_ID,
                        D.DATASET,
                        D.XTCROSSSECTION,
                        D.CREATION_DATE,
                        D.CREATE_BY,
                        D.LAST_MODIFICATION_DATE,
                        D.LAST_MODIFIED_BY,
                        P.PRIMARY_DS_NAME,
                        PDT.PRIMARY_DS_TYPE,
                        PD.PROCESSED_DS_NAME,
                        DT.DATA_TIER_NAME,
                        DP.DATASET_ACCESS_TYPE,
                        AE.ACQUISITION_ERA_NAME,
                        D.PROCESSING_ERA_ID,
                        PH.PHYSICS_GROUP_NAME,
                        D.PREP_ID
                        FROM {ownerDBS3}.DATASETS D
                        JOIN {ownerDBS3}.PRIMARY_DATASETS P ON P.PRIMARY_DS_ID = D.PRIMARY_DS_ID
                        JOIN {ownerDBS3}.PRIMARY_DS_TYPES PDT ON PDT.PRIMARY_DS_TYPE_ID = P.PRIMARY_DS_TYPE_ID
                        JOIN {ownerDBS3}.PROCESSED_DATASETS PD ON PD.PROCESSED_DS_ID = D.PROCESSED_DS_ID
                        JOIN {ownerDBS3}.DATA_TIERS DT ON DT.DATA_TIER_ID = D.DATA_TIER_ID
                        JOIN {ownerDBS3}.DATASET_ACCESS_TYPES DP on DP.DATASET_ACCESS_TYPE_ID= D.DATASET_ACCESS_TYPE_ID
                        LEFT OUTER JOIN {ownerDBS3}.ACQUISITION_ERAS AE ON AE.ACQUISITION_ERA_ID = D.ACQUISITION_ERA_ID
                        LEFT OUTER JOIN {ownerDBS3}.PHYSICS_GROUPS PH ON PH.PHYSICS_GROUP_ID = D.PHYSICS_GROUP_ID
                        UNION ALL
                        SELECT DS.ID DATASET_ID,
                        '/' || PD2.NAME || '/' || DS.NAME || '/' || DT2.NAME DATASET,
                        DS.XTCROSSSECTION,
                        DS.CREATIONDATE CREATION_DATE,
                        PS1.DISTINGUISHEDNAME CREATE_BY,
                        DS.LASTMODIFICATIONDATE LAST_MODIFICATION_DATE,
                        PS2.DISTINGUISHEDNAME LAST_MODIFIED_BY,
                        PD2.NAME PRIMARY_DS_NAME,
                        PT.TYPE PRIMARY_DS_TYPE,
                        DS.NAME PROCESSED_DS_NAME,
                        DT2.NAME DATA_TIER_NAME,
                        ST.STATUS DATASET_ACCESS_TYPE,
                        DS.AQUISITIONERA ACQUISITION_ERA_NAME,
                        NULL PROCESSING_ERA_ID,
                        PG.PHYSICSGROUPNAME physics_group_name,
                        NULL PREP_ID
                        FROM {ownerDBS2}.PROCESSEDDATASET DS
                        JOIN {ownerDBS2}.DATATIER DT2 ON DS.DATATIER=DT2.ID
                        JOIN {ownerDBS2}.PRIMARYDATASET PD2 ON PD2.ID=DS.PRIMARYDATASET
                        JOIN {ownerDBS2}.PHYSICSGROUP PG ON PG.ID=DS.PHYSICSGROUP
                        JOIN {ownerDBS2}.PROCDSSTATUS ST ON ST.ID=DS.STATUS
                        JOIN {ownerDBS2}.PERSON PS1 ON DS.CREATEDBY=PS1.ID
                        JOIN {ownerDBS2}.PERSON PS2 ON DS.LASTMODIFIEDBY=PS2.ID
                        JOIN {ownerDBS2}.PRIMARYDSTYPE PT ON PT.ID=PD2.TYPE
                        )
                        GROUP BY DATASET_ID,
                        DATASET,
                        XTCROSSSECTION,
                        CREATION_DATE,
                        CREATE_BY,
                        LAST_MODIFICATION_DATE,
                        LAST_MODIFIED_BY,
                        PRIMARY_DS_NAME,
                        PRIMARY_DS_TYPE,
                        PROCESSED_DS_NAME,
                        DATA_TIER_NAME,
                        DATASET_ACCESS_TYPE,
                        ACQUISITION_ERA_NAME,
                        PROCESSING_ERA_ID,
                        PHYSICS_GROUP_NAME,
                        PREP_ID
                        HAVING COUNT(*) <> 2
                        ORDER BY DATASET_ID
                        """.format(ownerDBS3=ownerDBS3, ownerDBS2=ownerDBS2),
                        ##############################################
                        ## Some datatypes are not existing anymore in DBS3
                        'DatasetAccessTypes':
                        """SELECT DATASET_ACCESS_TYPE_ID,
                        DATASET_ACCESS_TYPE
                        FROM(
                        SELECT DAT.DATASET_ACCESS_TYPE_ID,
                        DAT.DATASET_ACCESS_TYPE
                        FROM {ownerDBS3}.DATASET_ACCESS_TYPES DAT
                        UNION ALL
                        SELECT PDS.ID DATASET_ACCESS_TYPE_ID,
                        PDS.STATUS DATASET_ACCESS_TYPE
                        FROM {ownerDBS2}.PROCDSSTATUS PDS
                        WHERE PDS.ID!=3 AND PDS.ID!=4 AND PDS.ID!=21 AND PDS.ID!=61 
                        )
                        GROUP BY DATASET_ACCESS_TYPE_ID,
                        DATASET_ACCESS_TYPE
                        HAVING COUNT(*) <> 2
                        ORDER BY DATASET_ACCESS_TYPE_ID
                        """.format(ownerDBS3=ownerDBS3, ownerDBS2=ownerDBS2),
                        ##############################################
                        'DatasetOutputModConfigs':
                        """SELECT DS_OUTPUT_MOD_CONF_ID,
                        DATASET_ID,
                        OUTPUT_MOD_CONFIG_ID
                        FROM(
                        SELECT DOMC.DS_OUTPUT_MOD_CONF_ID,
                        DOMC.DATASET_ID,
                        DOMC.OUTPUT_MOD_CONFIG_ID
                        FROM {ownerDBS3}.DATASET_OUTPUT_MOD_CONFIGS DOMC
                        UNION ALL
                        SELECT PA.ID ds_output_mod_conf_id,
                        PA.DATASET dataset_id,
                        PA.ALGORITHM output_mod_config_id
                        FROM {ownerDBS2}.PROCALGO PA
                        )
                        GROUP BY DS_OUTPUT_MOD_CONF_ID,
                        DATASET_ID,
                        OUTPUT_MOD_CONFIG_ID
                        HAVING COUNT(*) <> 2
                        ORDER BY DS_OUTPUT_MOD_CONF_ID
                        """.format(ownerDBS3=ownerDBS3, ownerDBS2=ownerDBS2),
                        ##############################################
                        'DatasetParents':
                        """SELECT THIS_DATASET_ID,
                        PARENT_DATASET_ID
                        FROM(
                        SELECT DP.THIS_DATASET_ID,
                        DP.PARENT_DATASET_ID
                        FROM {ownerDBS3}.DATASET_PARENTS DP
                        UNION ALL
                        SELECT DP2.THISDATASET this_dataset_id,
                        DP2.ITSPARENT parent_dataset_id
                        FROM {ownerDBS2}.PROCDSPARENT DP2
                        )
                        GROUP BY THIS_DATASET_ID,
                        PARENT_DATASET_ID
                        HAVING COUNT(*) <> 2
                        ORDER BY this_dataset_id,parent_dataset_id
                        """.format(ownerDBS3=ownerDBS3, ownerDBS2=ownerDBS2),
                        ##############################################    
                        'File':
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
                        FROM {ownerDBS3}.FILES F
                        JOIN {ownerDBS3}.FILE_DATA_TYPES FT ON  FT.FILE_TYPE_ID = F.FILE_TYPE_ID
                        JOIN {ownerDBS3}.DATASETS D ON  D.DATASET_ID = F.DATASET_ID
                        JOIN {ownerDBS3}.BLOCKS B ON B.BLOCK_ID = F.BLOCK_ID
                        UNION ALL
                        SELECT
                        FS2.ID file_id,
                        FS2.LOGICALFILENAME logical_file_name,
                        CASE
                        WHEN FST.STATUS='VALID'
                        THEN 1
                        ELSE 0 END AS IS_FILE_VALID, 
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
                        FROM {ownerDBS2}.FILES FS2
                        JOIN {ownerDBS2}.PROCESSEDDATASET DS2 ON DS2.ID=FS2.DATASET
                        JOIN {ownerDBS2}.PRIMARYDATASET PD2 on DS2.PRIMARYDATASET=PD2.ID
                        JOIN {ownerDBS2}.DATATIER DT2 ON DS2.DATATIER=DT2.ID
                        JOIN {ownerDBS2}.PERSON PS12 ON FS2.CREATEDBY=PS12.ID
                        JOIN {ownerDBS2}.PERSON PS22 ON FS2.LASTMODIFIEDBY=PS22.ID
                        JOIN {ownerDBS2}.BLOCK BL2 ON FS2.BLOCK=BL2.ID
                        JOIN {ownerDBS2}.FILETYPE FT2 ON FT2.ID=FS2.FILETYPE
                        JOIN {ownerDBS2}.FILESTATUS FST ON FST.ID=FS2.FILESTATUS
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
                        HAVING COUNT(*) <> 2
                        ORDER BY FILE_ID
                        """.format(ownerDBS3=ownerDBS3, ownerDBS2=ownerDBS2),
                        ##############################################
                        'FileDataTypes':
                        """SELECT FILE_TYPE_ID,
                        FILE_TYPE
                        FROM(
                        SELECT FDT.FILE_TYPE_ID,
                        FDT.FILE_TYPE
                        FROM {ownerDBS3}.FILE_DATA_TYPES FDT
                        UNION ALL
                        SELECT FDT2.ID FILE_TYPE_ID,
                        FDT2.TYPE FILE_TYPE
                        FROM {ownerDBS2}.FILETYPE FDT2
                        )
                        GROUP BY FILE_TYPE_ID,
                        FILE_TYPE
                        HAVING COUNT(*) <> 2
                        ORDER BY FILE_TYPE_ID
                        """.format(ownerDBS3=ownerDBS3, ownerDBS2=ownerDBS2),
                        ##############################################
                        'FileLumis':
                        """SELECT RUN_NUM,LUMI_SECTION_NUM,FILE_ID FROM
                        (SELECT FL.RUN_NUM,FL.LUMI_SECTION_NUM,FL.FILE_ID
                        FROM {ownerDBS3}.FILE_LUMIS FL
                        UNION ALL
                        SELECT RU.RUNNUMBER RUN_NUM, LU.LUMISECTIONNUMBER LUMI_SECTION_NUM, FRL.FILEID FILE_ID
                        FROM {ownerDBS2}.FILERUNLUMI FRL
                        JOIN {ownerDBS2}.RUNS RU ON FRL.RUN=RU.ID
                        JOIN {ownerDBS2}.LUMISECTION LU ON FRL.LUMI=LU.ID
                        )
                        GROUP BY RUN_NUM,LUMI_SECTION_NUM,FILE_ID
                        HAVING COUNT(*) <> 2
                        ORDER BY FILE_ID
                        """.format(ownerDBS3=ownerDBS3, ownerDBS2=ownerDBS2),
                        ##############################################
                        'FileLumisMinMax':
                        """SELECT MIN(FRL.FILEID) AS MIN_ID,
                        MAX(FRL.FILEID) AS MAX_ID
                        FROM {ownerDBS2}.FILERUNLUMI FRL
                        """.format(ownerDBS2=ownerDBS2),
                        ##############################################
                        'FileLumisSplited':
                        """SELECT RUN_NUM,LUMI_SECTION_NUM,FILE_ID FROM
                        (SELECT FL.RUN_NUM,FL.LUMI_SECTION_NUM,FL.FILE_ID
                        FROM {ownerDBS3}.FILE_LUMIS FL
                        UNION ALL
                        SELECT RU.RUNNUMBER RUN_NUM, LU.LUMISECTIONNUMBER LUMI_SECTION_NUM, FRL.FILEID file_id
                        FROM {ownerDBS2}.FILERUNLUMI FRL
                        JOIN {ownerDBS2}.RUNS RU ON FRL.RUN=RU.ID
                        JOIN {ownerDBS2}.LUMISECTION LU ON FRL.LUMI=LU.ID
                        )
                        WHERE FILE_ID >= :min_id AND FILE_ID <= :max_id
                        GROUP BY RUN_NUM,LUMI_SECTION_NUM,FILE_ID
                        HAVING COUNT(*) <> 2
                        ORDER BY FILE_ID
                        """.format(ownerDBS3=ownerDBS3, ownerDBS2=ownerDBS2),
                        ##############################################
                        'FileOutputModConfigs':
                        """SELECT FILE_OUTPUT_CONFIG_ID,FILE_ID,OUTPUT_MOD_CONFIG_ID FROM
                        (SELECT FOMC.FILE_OUTPUT_CONFIG_ID,FOMC.FILE_ID,FOMC.OUTPUT_MOD_CONFIG_ID
                        FROM {ownerDBS3}.FILE_OUTPUT_MOD_CONFIGS FOMC
                        UNION ALL
                        SELECT FA.ID file_output_config_id, FA.FILEID file_id, FA.ALGORITHM output_mod_config_id
                        FROM {ownerDBS2}.FILEALGO FA
                        )
                        GROUP BY FILE_OUTPUT_CONFIG_ID,FILE_ID,OUTPUT_MOD_CONFIG_ID
                        HAVING COUNT(*) <> 2
                        ORDER BY FILE_OUTPUT_CONFIG_ID
                        """.format(ownerDBS3=ownerDBS3, ownerDBS2=ownerDBS2),
                        ##############################################
                        'FileParents':
                        """SELECT THIS_FILE_ID,PARENT_FILE_ID FROM
                        (SELECT FP.THIS_FILE_ID,FP.PARENT_FILE_ID
                        FROM {ownerDBS3}.FILE_PARENTS FP
                        UNION ALL
                        SELECT FP2.THISFILE this_file_id, FP2.ITSPARENT parent_file_id
                        FROM {ownerDBS2}.FILEPARENTAGE FP2)
                        GROUP BY THIS_FILE_ID,PARENT_FILE_ID
                        HAVING COUNT(*) <> 2
                        ORDER BY THIS_FILE_ID,PARENT_FILE_ID
                        """.format(ownerDBS3=ownerDBS3, ownerDBS2=ownerDBS2),
                        ##############################################
                        'OriginSiteName':
                        """SELECT BLOCK_ID, ORIGIN_SITE_NAME FROM
                        (SELECT DISTINCT B3.BLOCK_ID, B3.ORIGIN_SITE_NAME FROM {ownerDBS3}.BLOCKS B3
                        WHERE B3.ORIGIN_SITE_NAME!='UNKNOWN'
                        UNION ALL
                        SELECT DISTINCT B1.ID AS BLOCK_ID,
                        SE1.SENAME AS ORIGIN_SITE_NAME
                        FROM {ownerDBS2}.BLOCK B1
                        JOIN {ownerDBS2}.SEBLOCK SEB1 ON B1.ID=SEB1.BLOCKID
                        JOIN {ownerDBS2}.STORAGEELEMENT SE1 ON SEB1.SEID=SE1.ID
                        WHERE B1.ID IN (SELECT B2.ID FROM {ownerDBS2}.BLOCK B2
                        JOIN {ownerDBS2}.SEBLOCK SEB2 ON B2.ID=SEB2.BLOCKID
                        JOIN {ownerDBS2}.STORAGEELEMENT SE2 ON SEB2.SEID=SE2.ID
                        WHERE B2.ID=B1.ID GROUP BY B2.ID HAVING COUNT(B2.ID)=1)
                        )
                        GROUP BY BLOCK_ID, ORIGIN_SITE_NAME
                        HAVING COUNT(*)<>2
                        ORDER BY BLOCK_ID
                        """.format(ownerDBS3=ownerDBS3, ownerDBS2=ownerDBS2),
                        ##############################################
                        'OutputModule':
                        """SELECT OUTPUT_MOD_CONFIG_ID,
                        APP_NAME,
                        RELEASE_VERSION,
                        PARAMETER_SET_HASH_ID,
                        PSET_HASH,
                        pset_name,
                        OUTPUT_MODULE_LABEL,
                        GLOBAL_TAG,
                        SCENARIO,
                        CREATION_DATE,
                        CREATE_BY
                        FROM(
                        SELECT O.OUTPUT_MOD_CONFIG_ID,
                        AE.APP_NAME,
                        RV.RELEASE_VERSION,
                        PSH.PARAMETER_SET_HASH_ID,
                        PSH.PSET_HASH,
                        PSH.PSET_NAME,
                        O.OUTPUT_MODULE_LABEL,
                        O.GLOBAL_TAG,
                        O.SCENARIO,
                        O.CREATION_DATE,
                        O.CREATE_BY
                        FROM {ownerDBS3}.OUTPUT_MODULE_CONFIGS O
                        JOIN {ownerDBS3}.APPLICATION_EXECUTABLES AE ON O.APP_EXEC_ID=AE.APP_EXEC_ID
                        JOIN {ownerDBS3}.RELEASE_VERSIONS RV ON O.RELEASE_VERSION_ID=RV.RELEASE_VERSION_ID
                        JOIN {ownerDBS3}.PARAMETER_SET_HASHES PSH ON O.PARAMETER_SET_HASH_ID=PSH.PARAMETER_SET_HASH_ID
                        UNION ALL
                        SELECT DISTINCT AC.ID OUTPUT_MOD_CONFIG_ID,
                        APPEX.EXECUTABLENAME APP_NAME,
                        APPVER.VERSION RELEASE_VERSION,
                        AC.PARAMETERSETID PARAMETER_SET_HASH_ID,
                        QPS.HASH PSET_HASH,
                        QPS.NAME PSET_NAME,
                        TO_CHAR(AC.APPLICATIONFAMILY) OUTPUT_MODULE_LABEL,
                        CASE
                        WHEN (SELECT COUNT(DISTINCT PDS.GLOBALTAG) 
                        FROM {ownerDBS2}.PROCALGO PA
                        INNER JOIN {ownerDBS2}.PROCESSEDDATASET PDS ON PA.DATASET = PDS.ID
                        INNER JOIN {ownerDBS2}.ALGORITHMCONFIG AC2 on AC2.ID = PA.ALGORITHM
                        WHERE PDS.GLOBALTAG IS NOT NULL
                        ) = 1  
                        THEN (SELECT DISTINCT PDS.GLOBALTAG
                        FROM {ownerDBS2}.PROCALGO PA
                        LEFT JOIN {ownerDBS2}.PROCESSEDDATASET PDS ON PA.DATASET = PDS.ID
                        WHERE PDS.GLOBALTAG IS NOT NULL AND AC.ID = PA.ALGORITHM)
                        ELSE 'UNKNOWN'
                        END AS GLOBAL_TAG,
                        NULL SCENARIO,
                        AC.CREATIONDATE CREATION_DATE,
                        PS.DISTINGUISHEDNAME CREATE_BY
                        FROM {ownerDBS2}.ALGORITHMCONFIG AC
                        JOIN {ownerDBS2}.APPEXECUTABLE APPEX ON APPEX.ID=AC.EXECUTABLENAME
                        JOIN {ownerDBS2}.APPVERSION APPVER ON APPVER.ID=AC.APPLICATIONVERSION
                        JOIN {ownerDBS2}.PERSON PS ON PS.ID=AC.CREATEDBY
                        JOIN {ownerDBS2}.QUERYABLEPARAMETERSET QPS ON QPS.ID=AC.PARAMETERSETID
                        )
                        GROUP BY OUTPUT_MOD_CONFIG_ID,
                        APP_NAME,
                        RELEASE_VERSION,
                        PARAMETER_SET_HASH_ID,
                        PSET_HASH,
                        PSET_NAME,
                        OUTPUT_MODULE_LABEL,
                        GLOBAL_TAG,
                        SCENARIO,
                        CREATION_DATE,
                        CREATE_BY
                        HAVING COUNT(*) <> 2
                        ORDER BY OUTPUT_MOD_CONFIG_ID
                        """.format(ownerDBS3=ownerDBS3, ownerDBS2=ownerDBS2),
                        ##############################################
                        'ParametersetHashes':
                        """
                        SELECT PARAMETER_SET_HASH_ID,
                        PSET_HASH,
                        PSET_NAME
                        FROM(
                        SELECT PH.PARAMETER_SET_HASH_ID,
                        PH.PSET_HASH,
                        PH.PSET_NAME
                        FROM {ownerDBS3}.PARAMETER_SET_HASHES PH
                        UNION ALL
                        SELECT QP.ID PARAMETER_SET_HASH_ID,
                        QP.HASH PSET_HASH,
                        QP.NAME PSET_NAME
                        FROM {ownerDBS2}.QUERYABLEPARAMETERSET QP
                        )
                        GROUP BY PARAMETER_SET_HASH_ID,
                        PSET_HASH,
                        PSET_NAME
                        HAVING COUNT(*) <> 2
                        ORDER BY PARAMETER_SET_HASH_ID
                        """.format(ownerDBS3=ownerDBS3, ownerDBS2=ownerDBS2),
                        ##############################################
                        'PhysicsGroups':
                        """SELECT PHYSICS_GROUP_ID,
                        PHYSICS_GROUP_NAME
                        FROM(
                        SELECT PG.PHYSICS_GROUP_ID,
                        PG.PHYSICS_GROUP_NAME
                        FROM {ownerDBS3}.PHYSICS_GROUPS PG
                        UNION ALL
                        SELECT PG2.ID PHYSICS_GROUP_ID,
                        PG2.PHYSICSGROUPNAME PHYSICS_GROUP_NAME
                        FROM {ownerDBS2}.PHYSICSGROUP PG2
                        )
                        GROUP BY PHYSICS_GROUP_ID,
                        PHYSICS_GROUP_NAME
                        HAVING COUNT(*) <> 2
                        ORDER BY PHYSICS_GROUP_ID
                        """.format(ownerDBS3=ownerDBS3, ownerDBS2=ownerDBS2),
                        ##############################################
                        'PrimaryDS':
                        """SELECT PRIMARY_DS_ID,
                        PRIMARY_DS_NAME,
                        CREATION_DATE,
                        CREATE_BY,
                        PRIMARY_DS_TYPE
                        FROM(
                        SELECT P.PRIMARY_DS_ID,
                        P.PRIMARY_DS_NAME,
                        P.CREATION_DATE,
                        P.CREATE_BY,
                        PT.PRIMARY_DS_TYPE
                        FROM {ownerDBS3}.PRIMARY_DATASETS P
                        JOIN {ownerDBS3}.PRIMARY_DS_TYPES PT ON PT.PRIMARY_DS_TYPE_ID = P.PRIMARY_DS_TYPE_ID
                        UNION ALL
                        SELECT PD.ID PRIMARY_DS_ID,
                        PD.NAME PRIMARY_DS_NAME,
                        PD.CREATIONDATE CREATION_DATE,
                        PS.DISTINGUISHEDNAME CREATE_BY,
                        PT2.TYPE PRIMARY_DS_TYPE
                        FROM {ownerDBS2}.PRIMARYDATASET PD
                        JOIN {ownerDBS2}.PERSON PS ON PS.ID=PD.CREATEDBY
                        JOIN {ownerDBS2}.PRIMARYDSTYPE PT2 ON PT2.ID=PD.TYPE
                        )
                        GROUP BY PRIMARY_DS_ID, 
                        PRIMARY_DS_NAME,
                        CREATION_DATE,
                        CREATE_BY,
                        PRIMARY_DS_TYPE
                        HAVING COUNT(*) <> 2
                        ORDER BY PRIMARY_DS_ID
                        """.format(ownerDBS3=ownerDBS3, ownerDBS2=ownerDBS2),
                        ##############################################
                        'PrimaryDSTypes':
                        """SELECT PRIMARY_DS_TYPE_ID,
                        PRIMARY_DS_TYPE
                        FROM(
                        SELECT PDST.PRIMARY_DS_TYPE_ID,
                        PDST.PRIMARY_DS_TYPE
                        FROM {ownerDBS3}.PRIMARY_DS_TYPES PDST
                        UNION ALL
                        SELECT PDST.ID PRIMARY_DS_TYPE_ID,
                        PDST.TYPE PRIMARY_DS_TYPE
                        FROM {ownerDBS2}.PRIMARYDSTYPE PDST
                        )
                        GROUP BY PRIMARY_DS_TYPE_ID,
                        PRIMARY_DS_TYPE
                        HAVING COUNT(*) <> 2
                        ORDER BY PRIMARY_DS_TYPE_ID
                        """.format(ownerDBS3=ownerDBS3, ownerDBS2=ownerDBS2),
                        ##############################################
                        'ProcessedDatasets':
                        """SELECT PROCESSED_DS_NAME
                        FROM(
                        SELECT DISTINCT PCD.PROCESSED_DS_NAME
                        FROM {ownerDBS3}.PROCESSED_DATASETS PCD
                        UNION ALL
                        SELECT DISTINCT PCD2.NAME PROCESSED_DS_NAME
                        FROM {ownerDBS2}.PROCESSEDDATASET PCD2
                        )
                        GROUP BY PROCESSED_DS_NAME
                        HAVING COUNT(*) <> 2
                        ORDER BY PROCESSED_DS_NAME
                        """.format(ownerDBS3=ownerDBS3, ownerDBS2=ownerDBS2),
                        ##############################################
                        'ReleaseVersions':
                        """
                        SELECT RELEASE_VERSION_ID,
                        RELEASE_VERSION
                        FROM (
                        SELECT RV.RELEASE_VERSION_ID,
                        RV.RELEASE_VERSION
                        FROM {ownerDBS3}.RELEASE_VERSIONS RV
                        UNION ALL
                        SELECT RV.ID RELEASE_VERSION_ID,
                        RV.VERSION RELEASE_VERSION
                        FROM {ownerDBS2}.APPVERSION RV
                        )
                        GROUP BY RELEASE_VERSION_ID,
                        RELEASE_VERSION
                        HAVING COUNT(*) <> 2
                        ORDER BY RELEASE_VERSION_ID
                        """.format(ownerDBS3=ownerDBS3, ownerDBS2=ownerDBS2),
                        }

    def acquisitionEras(self, sort=True):
        return self._queryDB('AcquisitionEras', sort=sort)

    def applicationExecutables(self, sort=True):
        return self._queryDB('ApplicationExecutables', sort=sort)
              
    def block(self, sort=True):
        return self._queryDB('Block', sort=sort)

    def blockParents(self, sort=True):
        return self._queryDB('BlockParents', sort=sort)

    def dataTier(self, sort=True):
        return self._queryDB('DataTier', sort=sort)

    def dataset(self, sort=True):
        return self._queryDB('Dataset', sort=sort)

    def datasetAccessTypes(self, sort=True):
        return self._queryDB('DatasetAccessTypes', sort=sort)

    def datasetOutputModConfigs(self, sort=True):
        return self._queryDB('DatasetOutputModConfigs', sort=sort)

    def datasetParents(self, sort=True):
        return self._queryDB('DatasetParents', sort=sort)

    def file(self, sort=True):
        return self._queryDB('File', sort=sort)

    def fileDataTypes(self, sort=True):
        return self._queryDB('FileDataTypes', sort=sort)

    def fileLumis(self, sort=True, split=None):
        if isinstance(split, int):
            result = self._queryDB('FileLumisMinMax', sort=False)

            if not len(result):
                return None
            
            min_id = result[0]['min_id']
            max_id = result[0]['max_id']

            stepwidth = (max_id-min_id)/split
                        
            retval = []
            
            for i in range(split):
                start = min_id + (i*stepwidth)

                print("Progress: %i" % (int(float(i*stepwidth*100)/float(max_id-min_id)))) 

                if i != (split-1):
                    bind_dict = {"min_id": start, "max_id": start+stepwidth}
                    retval += (self._queryDB('FileLumisSplited', binds=bind_dict, sort=sort))
                else:
                    bind_dict = {"min_id": min_id+(i*stepwidth), "max_id": max_id}
                    retval += (self._queryDB('FileLumisSplited', binds=bind_dict, sort=sort))

            return retval

        else:
            return self._queryDB('FileLumis', sort=sort)

    def fileOutputModConfigs(self, sort=True):
        return self._queryDB('FileOutputModConfigs', sort=sort)

    def fileParents(self, sort=True):
        return self._queryDB('FileParents', sort=sort)

    def originSiteName(self, sort=True):
        return self._queryDB('OriginSiteName', sort=sort)

    def outputModuleConfig(self, sort=True):
        return self._queryDB('OutputModule', sort=sort)

    def parametersetHashes(self, sort=True):
        return self._queryDB('ParametersetHashes', sort=sort)

    def physicsGroups(self, sort=True):
        return self._queryDB('PhysicsGroups', sort=sort)

    def primaryDataset(self, sort=True):
        return self._queryDB('PrimaryDS', sort=sort)

    def primaryDSTypes(self, sort=True):
        return self._queryDB('PrimaryDSTypes', sort=sort)

    def processedDatasets(self, sort=True):
        return self._queryDB('ProcessedDatasets', sort=sort)

    def releaseVersions(self, sort=True):
        return self._queryDB('ReleaseVersions', sort=sort)
    
    def _queryDB(self, query, binds=None, sort=True):
        if not binds:
            binds = {}

        connection = self.dbi.connection()
        try:
            cursors = self.dbi.processData(self.sqlDict[query],
                                           binds,
                                           connection,
                                           transaction=False,
                                           returnCursor=True)
        except:
            raise
        else:
            result = self.dbFormatter.formatCursor(cursors[0])
        finally:
            connection.close()

        if sort:
            return sorted(result, key=lambda entry: entry[self.sqlPrimaryKey[query]])
        else:
            return result
