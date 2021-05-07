#!/usr/bin/env python
"""
This scripts is used to check WMAgent data injected into DBS2 and DBS3 simutanously
"""

from optparse import OptionParser
from string import Template
import pprint, logging, os, sys, unittest

### need DB access
from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.DBFormatter import DBFormatter

###check for secrets
try:
    from DBSSecret import DBS2Secret
    from DBSSecret import DBS3Secret
except:
    msg = """You need to put a DBSSecret.py in your directory. It has to have the following structure:\n
                  DBS2Secret = {'connectUrl' : {
                                'reader' : 'oracle://reader:passwd@instance'
                                },
                                'databaseOwner' : 'owner'}
                  DBS3Secret = {'connectUrl' : {
                                'reader' : 'oracle://reader:passwd@instance'
                               },
                                'databaseOwner' : 'owner'}"""
    print(msg)

def get_command_line_options(executable_name, arguments):
    parser = OptionParser(usage="%s options" % executable_name)
    parser.add_option("-i", "--in", type="string", dest="input", help="Input File Containing datasets/blocks")
    parser.add_option("-d", "--datasets", action="store_true", dest="datasets", help="Input file contains datasets", default=True)
    parser.add_option("-b", "--blocks", action="store_true", dest="blocks", help="Input file contains blocks")

    (options, args) = parser.parse_args()

    error_msg = """You need to provide following options, --in=input.txt (mandatory)\n"""

    if not options.input:
        parser.print_help()
        parser.error(error_msg)

    return options

def get_blocks_from_dataset(dataset):
    ownerDBS3 = DBS3Secret['databaseOwner']
    connectUrlDBS3 = DBS3Secret['connectUrl']['reader']

    ownerDBS2 = DBS2Secret['databaseOwner']
    connectUrlDBS2 = DBS2Secret['connectUrl']['reader']

    db_query = DBQuery(connectUrlDBS2)

    sql_queries = SQLFactory(db_owner_dbs2=ownerDBS2,
                             db_owner_dbs3=ownerDBS3)

    return [this_block['block_name'] for this_block in db_query.execute(sql_queries.create_get_blocks_query(dataset))]

def get_lfns_from_block(block):
    ownerDBS3 = DBS3Secret['databaseOwner']
    connectUrlDBS3 = DBS3Secret['connectUrl']['reader']

    ownerDBS2 = DBS2Secret['databaseOwner']
    connectUrlDBS2 = DBS2Secret['connectUrl']['reader']

    db_query = DBQuery(connectUrlDBS2)

    sql_queries = SQLFactory(db_owner_dbs2=ownerDBS2,
                             db_owner_dbs3=ownerDBS3)

    return [this_file['logical_file_name'] for this_file in db_query.execute(sql_queries.create_get_files_query(block))]

class SQLFactory(object):
    def __init__(self, db_owner_dbs2, db_owner_dbs3):
        self._db_owner_dbs2 = db_owner_dbs2
        self._db_owner_dbs3 = db_owner_dbs3

        self.query_templates = {'Block':
                                Template("""
                                SELECT BLOCK_NAME,
                                PATH,
                                OPEN_FOR_WRITING,
                                BLOCK_SIZE,
                                FILE_COUNT
                                FROM(
                                SELECT BL.BLOCK_NAME,
                                DS.DATASET PATH,
                                BL.OPEN_FOR_WRITING,
                                BL.BLOCK_SIZE,
                                BL.FILE_COUNT
                                FROM {db_owner_dbs3}.BLOCKS BL
                                JOIN {db_owner_dbs3}.DATASETS DS ON BL.DATASET_ID=DS.DATASET_ID
                                WHERE BLOCK_NAME='$block'
                                UNION ALL
                                SELECT BL2.NAME BLOCK_NAME,
                                BL2.PATH,
                                BL2.OPENFORWRITING OPEN_FOR_WRITING,
                                BL2.BLOCKSIZE BLOCK_SIZE, BL2.NUMBEROFFILES FILE_COUNT
                                FROM {db_owner_dbs2}.BLOCK BL2
                                JOIN {db_owner_dbs2}.PROCESSEDDATASET DS ON DS.ID=BL2.DATASET
                                JOIN {db_owner_dbs2}.PRIMARYDATASET PD on DS.PRIMARYDATASET=PD.ID
                                JOIN {db_owner_dbs2}.DATATIER DT ON DS.DATATIER=DT.ID
                                WHERE BL2.NAME='$block'
                                )
                                GROUP BY
                                BLOCK_NAME,
                                PATH,
                                OPEN_FOR_WRITING,
                                BLOCK_SIZE,
                                FILE_COUNT
                                HAVING COUNT(*) = 1
                                ORDER BY BLOCK_NAME
                                """.format(db_owner_dbs2=db_owner_dbs2, db_owner_dbs3=db_owner_dbs3)),
                                ######################################################
                                'BlockParents':
                                Template("""
                                SELECT PARENT_BLOCK_NAME
                                FROM
                                (
                                SELECT PBL.BLOCK_NAME PARENT_BLOCK_NAME
                                FROM {db_owner_dbs3}.BLOCKS BL
                                JOIN {db_owner_dbs3}.BLOCK_PARENTS BLP ON BL.BLOCK_ID=BLP.THIS_BLOCK_ID
                                JOIN {db_owner_dbs3}.BLOCKS PBL ON BLP.PARENT_BLOCK_ID=PBL.BLOCK_ID
                                WHERE BL.BLOCK_NAME='$block'
                                UNION ALL
                                SELECT PBL2.NAME PARENT_BLOCK_NAME
                                FROM {db_owner_dbs2}.BLOCK BL2
                                JOIN {db_owner_dbs2}.BLOCKPARENT BLP2 ON BL2.ID=BLP2.THISBLOCK
                                JOIN {db_owner_dbs2}.BLOCK PBL2 ON PBL2.ID=BLP2.ITSPARENT
                                WHERE BL2.NAME='$block'
                                )
                                GROUP BY PARENT_BLOCK_NAME
                                HAVING COUNT(*) = 1
                                ORDER BY PARENT_BLOCK_NAME
                                """.format(db_owner_dbs2=db_owner_dbs2, db_owner_dbs3=db_owner_dbs3)),
                                ######################################################
                                'Dataset':
                                Template("""
                                SELECT DATASET,
                                XTCROSSSECTION,
                                PRIMARY_DS_NAME,
                                PRIMARY_DS_TYPE,
                                PROCESSED_DS_NAME,
                                DATA_TIER_NAME,
                                DATASET_ACCESS_TYPE,
                                ACQUISITION_ERA_NAME,
                                PHYSICS_GROUP_NAME,
                                PREP_ID
                                FROM(
                                SELECT D.DATASET,
                                D.XTCROSSSECTION,
                                P.PRIMARY_DS_NAME,
                                LOWER(PDT.PRIMARY_DS_TYPE) PRIMARY_DS_TYPE,
                                PD.PROCESSED_DS_NAME,
                                DT.DATA_TIER_NAME,
                                DP.DATASET_ACCESS_TYPE,
                                CASE
                                WHEN (SELECT
                                DS.AQUISITIONERA ACQUISITION_ERA_NAME
                                FROM {db_owner_dbs2}.PROCESSEDDATASET DS
                                JOIN {db_owner_dbs2}.DATATIER DT2 ON DS.DATATIER=DT2.ID
                                JOIN {db_owner_dbs2}.PRIMARYDATASET PD2 ON PD2.ID=DS.PRIMARYDATASET
                                WHERE PD2.NAME='$primary_ds' and DS.NAME='$processed_ds' and DT2.NAME='$tier'
                                ) IS NOT NULL
                                THEN AE.ACQUISITION_ERA_NAME
                                ELSE NULL
                                END AS ACQUISITION_ERA_NAME,
                                PH.PHYSICS_GROUP_NAME,
                                D.PREP_ID
                                FROM {db_owner_dbs3}.DATASETS D
                                JOIN {db_owner_dbs3}.PRIMARY_DATASETS P ON P.PRIMARY_DS_ID = D.PRIMARY_DS_ID
                                JOIN {db_owner_dbs3}.PRIMARY_DS_TYPES PDT ON PDT.PRIMARY_DS_TYPE_ID = P.PRIMARY_DS_TYPE_ID
                                JOIN {db_owner_dbs3}.PROCESSED_DATASETS PD ON PD.PROCESSED_DS_ID = D.PROCESSED_DS_ID
                                JOIN {db_owner_dbs3}.DATA_TIERS DT ON DT.DATA_TIER_ID = D.DATA_TIER_ID
                                JOIN {db_owner_dbs3}.DATASET_ACCESS_TYPES DP on DP.DATASET_ACCESS_TYPE_ID= D.DATASET_ACCESS_TYPE_ID
                                JOIN {db_owner_dbs3}.ACQUISITION_ERAS AE ON AE.ACQUISITION_ERA_ID = D.ACQUISITION_ERA_ID
                                LEFT OUTER JOIN {db_owner_dbs3}.PHYSICS_GROUPS PH ON PH.PHYSICS_GROUP_ID = D.PHYSICS_GROUP_ID
                                WHERE DATASET='/$primary_ds/$processed_ds/$tier'
                                UNION ALL
                                SELECT '/' || PD2.NAME || '/' || DS.NAME || '/' || DT2.NAME DATASET,
                                DS.XTCROSSSECTION,
                                PD2.NAME PRIMARY_DS_NAME,
                                PT.TYPE PRIMARY_DS_TYPE,
                                DS.NAME PROCESSED_DS_NAME,
                                DT2.NAME DATA_TIER_NAME,
                                ST.STATUS DATASET_ACCESS_TYPE,
                                DS.AQUISITIONERA ACQUISITION_ERA_NAME,
                                PG.PHYSICSGROUPNAME physics_group_name,
                                NULL PREP_ID
                                FROM {db_owner_dbs2}.PROCESSEDDATASET DS
                                JOIN {db_owner_dbs2}.DATATIER DT2 ON DS.DATATIER=DT2.ID
                                JOIN {db_owner_dbs2}.PRIMARYDATASET PD2 ON PD2.ID=DS.PRIMARYDATASET
                                JOIN {db_owner_dbs2}.PHYSICSGROUP PG ON PG.ID=DS.PHYSICSGROUP
                                JOIN {db_owner_dbs2}.PROCDSSTATUS ST ON ST.ID=DS.STATUS
                                JOIN {db_owner_dbs2}.PRIMARYDSTYPE PT ON PT.ID=PD2.TYPE
                                WHERE PD2.NAME='$primary_ds' and DS.NAME='$processed_ds' and DT2.NAME='$tier'
                                )
                                GROUP BY DATASET,
                                XTCROSSSECTION,
                                PRIMARY_DS_NAME,
                                PRIMARY_DS_TYPE,
                                PROCESSED_DS_NAME,
                                DATA_TIER_NAME,
                                DATASET_ACCESS_TYPE,
                                ACQUISITION_ERA_NAME,
                                PHYSICS_GROUP_NAME,
                                PREP_ID
                                HAVING COUNT(*) = 1
                                ORDER BY DATASET
                                """.format(db_owner_dbs2=db_owner_dbs2, db_owner_dbs3=db_owner_dbs3)),
                                ######################################################
                                'DatasetOutputModConfigs':
                                Template("""
                                SELECT
                                APP_NAME,
                                RELEASE_VERSION,
                                PSET_HASH,
                                PSET_NAME,
                                GLOBAL_TAG
                                FROM
                                (
                                SELECT DISTINCT APE.APP_NAME,
                                RV.RELEASE_VERSION,
                                PSH.PSET_HASH,
                                PSH.PSET_NAME,
                                CASE
                                WHEN (SELECT COUNT(DISTINCT PDS.GLOBALTAG)
                                FROM cms_dbs_prod_global.PROCALGO PA2
                                INNER JOIN cms_dbs_prod_global.PROCESSEDDATASET PDS ON PA2.DATASET = PDS.ID
                                INNER JOIN cms_dbs_prod_global.ALGORITHMCONFIG AC2 on AC2.ID = PA2.ALGORITHM
                                WHERE PDS.GLOBALTAG IS NOT NULL
                                ) = 1
                                THEN OMC.GLOBAL_TAG
                                ELSE 'UNKNOWN'
                                END AS GLOBAL_TAG
                                FROM {db_owner_dbs3}.DATASETS DS
                                JOIN {db_owner_dbs3}.DATASET_OUTPUT_MOD_CONFIGS DOMC ON DOMC.DATASET_ID=DS.DATASET_ID
                                JOIN {db_owner_dbs3}.OUTPUT_MODULE_CONFIGS OMC ON OMC.OUTPUT_MOD_CONFIG_ID=DOMC.OUTPUT_MOD_CONFIG_ID
                                JOIN {db_owner_dbs3}.APPLICATION_EXECUTABLES APE ON APE.APP_EXEC_ID=OMC.APP_EXEC_ID
                                JOIN {db_owner_dbs3}.RELEASE_VERSIONS RV ON RV.RELEASE_VERSION_ID=OMC.RELEASE_VERSION_ID
                                JOIN {db_owner_dbs3}.PARAMETER_SET_HASHES PSH ON PSH.PARAMETER_SET_HASH_ID=OMC.PARAMETER_SET_HASH_ID
                                WHERE DS.DATASET='/$primary_ds/$processed_ds/$tier'
                                UNION ALL
                                SELECT DISTINCT APPEX.EXECUTABLENAME APP_NAME,
                                APPVER.VERSION RELEASE_VERSION,
                                QPS.HASH PSET_HASH,
                                QPS.NAME PSET_NAME,
                                CASE
                                WHEN (SELECT COUNT(DISTINCT PDS.GLOBALTAG)
                                FROM {db_owner_dbs2}.PROCALGO PA2
                                INNER JOIN {db_owner_dbs2}.PROCESSEDDATASET PDS ON PA2.DATASET = PDS.ID
                                INNER JOIN {db_owner_dbs2}.ALGORITHMCONFIG AC2 on AC2.ID = PA2.ALGORITHM
                                WHERE PDS.GLOBALTAG IS NOT NULL
                                ) = 1
                                THEN (SELECT DISTINCT PDS.GLOBALTAG
                                FROM {db_owner_dbs2}.PROCALGO PA2
                                INNER JOIN {db_owner_dbs2}.PROCESSEDDATASET PDS ON PA2.DATASET = PDS.ID
                                INNER JOIN {db_owner_dbs2}.ALGORITHMCONFIG AC2 on AC2.ID = PA2.ALGORITHM
                                WHERE PDS.GLOBALTAG IS NOT NULL)
                                ELSE 'UNKNOWN'
                                END AS GLOBAL_TAG
                                FROM {db_owner_dbs2}.PROCESSEDDATASET DS
                                JOIN {db_owner_dbs2}.DATATIER DT ON DS.DATATIER=DT.ID
                                JOIN {db_owner_dbs2}.PRIMARYDATASET PD ON PD.ID=DS.PRIMARYDATASET
                                JOIN {db_owner_dbs2}.PROCALGO PA ON PA.DATASET=DS.ID
                                JOIN {db_owner_dbs2}.ALGORITHMCONFIG AC ON AC.ID=PA.ALGORITHM
                                JOIN {db_owner_dbs2}.APPEXECUTABLE APPEX ON APPEX.ID=AC.EXECUTABLENAME
                                JOIN {db_owner_dbs2}.APPVERSION APPVER ON APPVER.ID=AC.APPLICATIONVERSION
                                JOIN {db_owner_dbs2}.QUERYABLEPARAMETERSET QPS ON QPS.ID=AC.PARAMETERSETID
                                WHERE PD.NAME='$primary_ds' AND DS.NAME='$processed_ds' AND DT.NAME='$tier')
                                GROUP BY APP_NAME,
                                RELEASE_VERSION,
                                PSET_HASH,
                                PSET_NAME,
                                GLOBAL_TAG
                                HAVING COUNT(*) = 1
                                ORDER BY RELEASE_VERSION
                                """.format(db_owner_dbs2=db_owner_dbs2, db_owner_dbs3=db_owner_dbs3)),
                                ######################################################
                                'DatasetParents':
                                Template("""
                                SELECT PARENT_DATASET_NAME
                                FROM
                                (
                                SELECT PDS.DATASET PARENT_DATASET_NAME
                                FROM {db_owner_dbs3}.DATASETS DS
                                JOIN {db_owner_dbs3}.DATASET_PARENTS DP ON DS.DATASET_ID = DP.THIS_DATASET_ID
                                JOIN {db_owner_dbs3}.DATASETS PDS ON DP.PARENT_DATASET_ID = PDS.DATASET_ID
                                WHERE DS.DATASET='/$primary_ds/$processed_ds/$tier'
                                UNION ALL
                                SELECT '/' || PPD.NAME || '/' || PDS.NAME || '/' || PDT.NAME PARENT_DATASET_NAME
                                FROM {db_owner_dbs2}.PROCESSEDDATASET DS
                                JOIN {db_owner_dbs2}.DATATIER DT ON DS.DATATIER=DT.ID
                                JOIN {db_owner_dbs2}.PRIMARYDATASET PD ON PD.ID=DS.PRIMARYDATASET
                                JOIN {db_owner_dbs2}.PROCDSPARENT DSP ON DSP.THISDATASET=DS.ID
                                JOIN {db_owner_dbs2}.PROCESSEDDATASET PDS ON PDS.ID = DSP.ITSPARENT
                                JOIN {db_owner_dbs2}.DATATIER PDT ON PDS.DATATIER=PDT.ID
                                JOIN {db_owner_dbs2}.PRIMARYDATASET PPD ON PPD.ID=PDS.PRIMARYDATASET
                                WHERE PD.NAME='$primary_ds' AND DS.NAME='$processed_ds' AND DT.NAME='$tier'
                                )
                                GROUP BY PARENT_DATASET_NAME
                                HAVING COUNT(*) = 1
                                ORDER BY PARENT_DATASET_NAME
                                """.format(db_owner_dbs2=db_owner_dbs2, db_owner_dbs3=db_owner_dbs3)),
                                ######################################################
                                'File':
                                Template("""
                                SELECT LOGICAL_FILE_NAME, IS_FILE_VALID,
                                DATASET,
                                BLOCK_NAME,
                                FILE_TYPE,
                                CHECK_SUM, EVENT_COUNT, FILE_SIZE,
                                BRANCH_HASH_ID,
                                ADLER32, MD5,
                                AUTO_CROSS_SECTION
                                FROM
                                (
                                SELECT F.LOGICAL_FILE_NAME, F.IS_FILE_VALID,
                                D.DATASET,
                                B.BLOCK_NAME,
                                FT.FILE_TYPE,
                                F.CHECK_SUM, F.EVENT_COUNT, F.FILE_SIZE,
                                F.BRANCH_HASH_ID, F.ADLER32, F.MD5,
                                NVL(F.AUTO_CROSS_SECTION, 0.0) auto_cross_section
                                FROM {db_owner_dbs3}.FILES F
                                JOIN {db_owner_dbs3}.FILE_DATA_TYPES FT ON  FT.FILE_TYPE_ID = F.FILE_TYPE_ID
                                JOIN {db_owner_dbs3}.DATASETS D ON  D.DATASET_ID = F.DATASET_ID
                                JOIN {db_owner_dbs3}.BLOCKS B ON B.BLOCK_ID = F.BLOCK_ID
                                WHERE LOGICAL_FILE_NAME='$lfn'
                                UNION ALL
                                SELECT
                                FS2.LOGICALFILENAME logical_file_name,
                                CASE
                                WHEN FST.STATUS='VALID'
                                THEN 1
                                ELSE 0 END AS is_file_valid,
                                '/' || PD2.NAME || '/' || DS2.NAME || '/' || DT2.NAME dataset,
                                BL2.NAME block_name,
                                FT2.TYPE file_type,
                                FS2.CHECKSUM check_sum,
                                FS2.NUMBEROFEVENTS event_count,
                                FS2.FILESIZE file_size,
                                FS2.FILEBRANCH branch_hash_id,
                                FS2.ADLER32,
                                FS2.MD5,
                                NVL(FS2.AUTOCROSSSECTION, 0.0) auto_cross_section
                                FROM {db_owner_dbs2}.FILES FS2
                                JOIN {db_owner_dbs2}.PROCESSEDDATASET DS2 ON DS2.ID=FS2.DATASET
                                JOIN {db_owner_dbs2}.PRIMARYDATASET PD2 on DS2.PRIMARYDATASET=PD2.ID
                                JOIN {db_owner_dbs2}.DATATIER DT2 ON DS2.DATATIER=DT2.ID
                                JOIN {db_owner_dbs2}.BLOCK BL2 ON FS2.BLOCK=BL2.ID
                                JOIN {db_owner_dbs2}.FILETYPE FT2 ON FT2.ID=FS2.FILETYPE
                                JOIN {db_owner_dbs2}.FILESTATUS FST ON FST.ID=FS2.FILESTATUS
                                WHERE FS2.LOGICALFILENAME='$lfn'
                                )
                                GROUP BY LOGICAL_FILE_NAME, IS_FILE_VALID,
                                DATASET,
                                BLOCK_NAME,
                                FILE_TYPE,
                                CHECK_SUM, EVENT_COUNT, FILE_SIZE,
                                BRANCH_HASH_ID, ADLER32, MD5,
                                AUTO_CROSS_SECTION
                                HAVING COUNT(*) = 1
                                ORDER BY LOGICAL_FILE_NAME
                                """.format(db_owner_dbs2=db_owner_dbs2, db_owner_dbs3=db_owner_dbs3)),
                                ######################################################
                                "FileLumis":
                                Template("""
                                SELECT RUN_NUM,LUMI_SECTION_NUM, LOGICAL_FILE_NAME FROM
                                (SELECT FL.RUN_NUM,FL.LUMI_SECTION_NUM,F.LOGICAL_FILE_NAME
                                FROM {db_owner_dbs3}.FILE_LUMIS FL
                                JOIN {db_owner_dbs3}.FILES F ON FL.FILE_ID=F.FILE_ID
                                WHERE F.LOGICAL_FILE_NAME='$lfn'
                                UNION ALL
                                SELECT RU.RUNNUMBER RUN_NUM, LU.LUMISECTIONNUMBER LUMI_SECTION_NUM, F.LOGICALFILENAME
                                FROM {db_owner_dbs2}.FILERUNLUMI FRL
                                JOIN {db_owner_dbs2}.RUNS RU ON FRL.RUN=RU.ID
                                JOIN {db_owner_dbs2}.LUMISECTION LU ON FRL.LUMI=LU.ID
                                JOIN {db_owner_dbs2}.FILES F on FRL.FILEID=F.ID
                                WHERE F.LOGICALFILENAME='$lfn'
                                )
                                GROUP BY RUN_NUM,LUMI_SECTION_NUM,LOGICAL_FILE_NAME
                                HAVING COUNT(*) = 1
                                ORDER BY LOGICAL_FILE_NAME
                                """.format(db_owner_dbs3=db_owner_dbs3, db_owner_dbs2=db_owner_dbs2)),
                                ######################################################
                                "FileParents":
                                Template("""
                                SELECT
                                PARENT_FILE_NAME
                                FROM
                                (
                                SELECT PFL.LOGICAL_FILE_NAME PARENT_FILE_NAME
                                FROM {db_owner_dbs3}.FILES FL
                                JOIN {db_owner_dbs3}.FILE_PARENTS FLP ON FL.FILE_ID = FLP.THIS_FILE_ID
                                JOIN {db_owner_dbs3}.FILES PFL ON PFL.FILE_ID = FLP.PARENT_FILE_ID
                                WHERE FL.LOGICAL_FILE_NAME='$lfn'
                                UNION ALL
                                SELECT PFL2.LOGICALFILENAME PARENT_FILE_NAME
                                FROM {db_owner_dbs2}.FILES FL2
                                JOIN {db_owner_dbs2}.FILEPARENTAGE FLP2 ON FLP2.THISFILE = FL2.ID
                                JOIN {db_owner_dbs2}.FILES PFL2 ON PFL2.ID = FLP2.ITSPARENT
                                WHERE FL2.LOGICALFILENAME='$lfn'
                                )
                                GROUP BY PARENT_FILE_NAME
                                HAVING COUNT(*) = 1
                                ORDER BY PARENT_FILE_NAME
                                """.format(db_owner_dbs3=db_owner_dbs3, db_owner_dbs2=db_owner_dbs2)),
                                ######################################################
                                'FileOutputModConfigs':
                                Template("""
                                SELECT
                                APP_NAME,
                                RELEASE_VERSION,
                                PSET_HASH,
                                PSET_NAME,
                                GLOBAL_TAG
                                FROM
                                (
                                SELECT APE.APP_NAME,
                                RV.RELEASE_VERSION,
                                PSH.PSET_HASH,
                                PSH.PSET_NAME,
                                CASE
                                WHEN (SELECT COUNT(DISTINCT PDS.GLOBALTAG)
                                FROM cms_dbs_prod_global.PROCALGO PA2
                                INNER JOIN {db_owner_dbs2}.PROCESSEDDATASET PDS ON PA2.DATASET = PDS.ID
                                INNER JOIN {db_owner_dbs2}.ALGORITHMCONFIG AC2 on AC2.ID = PA2.ALGORITHM
                                WHERE PDS.GLOBALTAG IS NOT NULL
                                ) = 1
                                THEN OMC.GLOBAL_TAG
                                ELSE 'UNKNOWN'
                                END AS GLOBAL_TAG
                                FROM {db_owner_dbs3}.FILES FL
                                JOIN {db_owner_dbs3}.FILE_OUTPUT_MOD_CONFIGS FOMC ON FOMC.FILE_ID=FL.FILE_ID
                                JOIN {db_owner_dbs3}.OUTPUT_MODULE_CONFIGS OMC ON OMC.OUTPUT_MOD_CONFIG_ID=FOMC.OUTPUT_MOD_CONFIG_ID
                                JOIN {db_owner_dbs3}.APPLICATION_EXECUTABLES APE ON APE.APP_EXEC_ID=OMC.APP_EXEC_ID
                                JOIN {db_owner_dbs3}.RELEASE_VERSIONS RV ON RV.RELEASE_VERSION_ID=OMC.RELEASE_VERSION_ID
                                JOIN {db_owner_dbs3}.PARAMETER_SET_HASHES PSH ON PSH.PARAMETER_SET_HASH_ID=OMC.PARAMETER_SET_HASH_ID
                                WHERE FL.LOGICAL_FILE_NAME='$lfn'
                                UNION ALL
                                SELECT APPEX.EXECUTABLENAME APP_NAME,
                                APPVER.VERSION RELEASE_VERSION,
                                QPS.HASH PSET_HASH,
                                QPS.NAME PSET_NAME,
                                CASE
                                WHEN (SELECT COUNT(DISTINCT PDS.GLOBALTAG)
                                FROM {db_owner_dbs2}.PROCALGO PA2
                                INNER JOIN {db_owner_dbs2}.PROCESSEDDATASET PDS ON PA2.DATASET = PDS.ID
                                INNER JOIN {db_owner_dbs2}.ALGORITHMCONFIG AC2 on AC2.ID = PA2.ALGORITHM
                                WHERE PDS.GLOBALTAG IS NOT NULL
                                ) = 1
                                THEN (SELECT DISTINCT PDS.GLOBALTAG
                                FROM {db_owner_dbs2}.PROCALGO PA2
                                LEFT JOIN {db_owner_dbs2}.PROCESSEDDATASET PDS ON PA2.DATASET = PDS.ID
                                LEFT JOIN {db_owner_dbs2}.ALGORITHMCONFIG AC2 on AC2.ID = PA2.ALGORITHM
                                WHERE PDS.GLOBALTAG IS NOT NULL)
                                ELSE 'UNKNOWN'
                                END AS GLOBAL_TAG
                                FROM {db_owner_dbs2}.FILES FL
                                JOIN {db_owner_dbs2}.FILEALGO FA ON FA.FILEID=FL.ID
                                JOIN {db_owner_dbs2}.ALGORITHMCONFIG AC ON AC.ID=FA.ALGORITHM
                                JOIN {db_owner_dbs2}.APPEXECUTABLE APPEX ON APPEX.ID=AC.EXECUTABLENAME
                                JOIN {db_owner_dbs2}.APPVERSION APPVER ON APPVER.ID=AC.APPLICATIONVERSION
                                JOIN {db_owner_dbs2}.QUERYABLEPARAMETERSET QPS ON QPS.ID=AC.PARAMETERSETID
                                WHERE FL.LOGICALFILENAME='$lfn'
                                )
                                GROUP BY APP_NAME,
                                RELEASE_VERSION,
                                PSET_HASH,
                                PSET_NAME,
                                GLOBAL_TAG
                                HAVING COUNT(*) = 1
                                ORDER BY RELEASE_VERSION
                                """.format(db_owner_dbs3=db_owner_dbs3, db_owner_dbs2=db_owner_dbs2)),
                                ######################################################
                                "GetListOfFiles":
                                Template("""
                                SELECT LOGICAL_FILE_NAME
                                FROM {db_owner_dbs3}.FILES F
                                JOIN {db_owner_dbs3}.BLOCKS B ON B.BLOCK_ID=F.BLOCK_ID
                                WHERE B.BLOCK_NAME='$block'
                                """.format(db_owner_dbs3=db_owner_dbs3)),
                                ######################################################
                                "GetListOfBlocks":
                                Template("""
                                SELECT BLOCK_NAME
                                FROM {db_owner_dbs3}.BLOCKS B
                                JOIN {db_owner_dbs3}.DATASETS D ON D.DATASET_ID=B.DATASET_ID
                                WHERE D.DATASET='$dataset'
                                """.format(db_owner_dbs3=db_owner_dbs3))
                                }

    def create_block_query(self, block):
        return self.query_templates['Block'].substitute(block=block)

    def create_block_parents_query(self, block):
        return self.query_templates['BlockParents'].substitute(block=block)

    def create_dataset_query(self, primary_ds, processed_ds, tier):
        return self.query_templates['Dataset'].substitute(primary_ds=primary_ds, processed_ds=processed_ds, tier=tier)

    def create_dataset_omc_query(self, primary_ds, processed_ds, tier):
        return self.query_templates['DatasetOutputModConfigs'].substitute(primary_ds=primary_ds, processed_ds=processed_ds, tier=tier)

    def create_dataset_parents_query(self, primary_ds, processed_ds, tier):
        return self.query_templates['DatasetParents'].substitute(primary_ds=primary_ds, processed_ds=processed_ds, tier=tier)

    def create_file_query(self, lfn):
        return self.query_templates['File'].substitute(lfn=lfn)

    def create_file_lumi_query(self, lfn):
        return self.query_templates['FileLumis'].substitute(lfn=lfn)

    def create_file_omc_query(self, lfn):
        return self.query_templates['FileOutputModConfigs'].substitute(lfn=lfn)

    def create_file_parents_query(self, lfn):
        return self.query_templates['FileParents'].substitute(lfn=lfn)

    def create_get_files_query(self, block):
        return self.query_templates['GetListOfFiles'].substitute(block=block)

    def create_get_blocks_query(self, dataset):
        return self.query_templates['GetListOfBlocks'].substitute(dataset=dataset)

class DBQuery(object):
    def __init__(self, connectUrl):
        logger = logging.getLogger()
        dbFactory = DBFactory(logger, connectUrl, options={})
        self.dbi = dbFactory.connect()
        self.dbFormatter = DBFormatter(logger, self.dbi)

    def execute(self, query, binds={}):
        connection = self.dbi.connection()

        cursors = self.dbi.processData(query,
                                       binds,
                                       connection,
                                       transaction=False,
                                       returnCursor=True)

        result = self.dbFormatter.formatCursor(cursors[0])

        connection.close()

        return result

class ValidateDualDBSWriting(unittest.TestCase):
    def __init__(self, methodName='runTest'):
        super(ValidateDualDBSWriting, self).__init__(methodName)
        self.ownerDBS3 = DBS3Secret['databaseOwner']
        self.connectUrlDBS3 = DBS3Secret['connectUrl']['reader']

        self.ownerDBS2 = DBS2Secret['databaseOwner']
        self.connectUrlDBS2 = DBS2Secret['connectUrl']['reader']

        self.db_query = DBQuery(self.connectUrlDBS2)

        self.sql_queries = SQLFactory(db_owner_dbs2=self.ownerDBS2,
                                      db_owner_dbs3=self.ownerDBS3)

class ValidateBlockData(ValidateDualDBSWriting):
    def __str__(self):
        """Override this so that we know which instance it is"""
        return "Testing block %s: %s" % (self.block, self._testMethodName)

    def test_blocks(self):
        results = self.db_query.execute(self.sql_queries.create_block_query(self.block))
        self.assertEqual(results, [], msg=pprint.pformat(results))

    def test_block_parents(self):
        results = self.db_query.execute(self.sql_queries.create_block_parents_query(self.block))
        self.assertEqual(results, [], msg=pprint.pformat(results))

class ValidateDatasetData(ValidateDualDBSWriting):
    def __str__(self):
        """Override this so that we know which instance it is"""
        return "Testing dataset %s: %s" % (self.dataset, self._testMethodName)

    def setUp(self):
        _, self.primary_dataset, self.processed_dataset, self.data_tier = self.dataset.split('/')

    def test_datasets(self):
        results = self.db_query.execute(self.sql_queries.create_dataset_query(self.primary_dataset,
                                                                              self.processed_dataset,
                                                                              self.data_tier))
        self.assertEqual(results, [], msg=pprint.pformat(results))

    def test_dataset_output_mod_config(self):
        results = self.db_query.execute(self.sql_queries.create_dataset_omc_query(self.primary_dataset,
                                                                                  self.processed_dataset,
                                                                                  self.data_tier))
        self.assertEqual(results, [], msg=pprint.pformat(results))

class ValidateFileData(ValidateDualDBSWriting):
    def __str__(self):
        """Override this so that we know which instance it is"""
        return "Testing lfn %s: %s" % (self.lfn, self._testMethodName)

    def test_files(self):
        results = self.db_query.execute(self.sql_queries.create_file_query(self.lfn))
        self.assertEqual(results, [], msg=pprint.pformat(results))

    def test_file_lumis(self):
        results = self.db_query.execute(self.sql_queries.create_file_lumi_query(self.lfn))
        self.assertEqual(results, [], msg=pprint.pformat(results))

    def test_file_output_mod_config(self):
        results = self.db_query.execute(self.sql_queries.create_file_omc_query(self.lfn))
        self.assertEqual(results, [], msg=pprint.pformat(results))

    def test_file_parents(self):
        results = self.db_query.execute(self.sql_queries.create_file_parents_query(self.lfn))
        self.assertEqual(results, [], msg=pprint.pformat(results))

class TestFactory(object):
    def create_dataset_test_cases(self, dataset):
        test_cases_datasets = unittest.TestLoader().loadTestsFromTestCase(ValidateDatasetData)
        for test_case in test_cases_datasets:
            test_case.dataset = dataset
        return test_cases_datasets

    def create_block_test_cases(self, block):
        test_cases_blocks = unittest.TestLoader().loadTestsFromTestCase(ValidateBlockData)
        for test_case in test_cases_blocks:
            test_case.block = block
        return test_cases_blocks

    def create_file_test_cases(self, this_file):
        test_cases_files = unittest.TestLoader().loadTestsFromTestCase(ValidateFileData)
        for test_case in test_cases_files:
            test_case.lfn = this_file
        return test_cases_files

if __name__ == '__main__':
    options = get_command_line_options(os.path.basename(__file__), sys.argv)
    test_cases_factory = TestFactory()

    with open(options.input, 'r') as f:
        unique_datasets = set()

        if options.blocks:
            for block in f:
                TestSuite = unittest.TestSuite()
                block = block.strip()
                dataset = block.split('#')[0]

                if dataset not in unique_datasets:
                    TestSuite.addTest(test_cases_factory.create_dataset_test_cases(dataset))
                    unique_datasets.add(dataset)

                TestSuite.addTest(test_cases_factory.create_block_test_cases(block))

                files = get_lfns_from_block(block)

                for this_file in files:
                    TestSuite.addTest(test_cases_factory.create_file_test_cases(this_file))

                unittest.TextTestRunner(verbosity=2).run(TestSuite)

        else:
            for dataset in f:
                TestSuite = unittest.TestSuite()
                dataset = dataset.strip()
                TestSuite.addTest(test_cases_factory.create_dataset_test_cases(dataset))

                blocks = get_blocks_from_dataset(dataset)
                for block in blocks:
                    block = block.strip()
                    TestSuite.addTest(test_cases_factory.create_block_test_cases(block))

                files = get_lfns_from_block(block)
                for this_file in files:
                    TestSuite.addTest(test_cases_factory.create_file_test_cases(this_file))

                unittest.TextTestRunner(verbosity=2).run(TestSuite)
