/* ---------------------------------------------------------------------- */
/* Script generated with: DeZign for Databases v5.2.3                     */
/* Target DBMS:           Oracle 10g                                      */
/* Project file:          DBS3-20091001.dez                               */
/* Project name:          DBS3                                            */
/* Author:                Yuyi Guo for DBS Group                          */
/* Script type:           Database drop script                            */
/* Created on:            2009-10-02 11:51                                */
/* Model version:         Version 2009-10-02                              */
/* ---------------------------------------------------------------------- */


/* ---------------------------------------------------------------------- */
/* Drop foreign key constraints                                           */
/* ---------------------------------------------------------------------- */

ALTER TABLE OUTPUT_MODULE_CONFIGS DROP CONSTRAINT AE_OMC;

ALTER TABLE OUTPUT_MODULE_CONFIGS DROP CONSTRAINT RV_OMC;

ALTER TABLE OUTPUT_MODULE_CONFIGS DROP CONSTRAINT PSH_OMC;

ALTER TABLE PRIMARY_DATASETS DROP CONSTRAINT PDT_PDS;

ALTER TABLE DATASETS DROP CONSTRAINT PDS_DS;

ALTER TABLE DATASETS DROP CONSTRAINT DT_DS;

ALTER TABLE DATASETS DROP CONSTRAINT PSDS_DS;

ALTER TABLE DATASETS DROP CONSTRAINT DTP_DS;

ALTER TABLE DATASETS DROP CONSTRAINT PG_DS;

ALTER TABLE DATASETS DROP CONSTRAINT AQE_DS;

ALTER TABLE DATASETS DROP CONSTRAINT PE_DS;

ALTER TABLE BLOCKS DROP CONSTRAINT DS_BK;

ALTER TABLE BLOCKS DROP CONSTRAINT SI_BK;

ALTER TABLE BLOCK_PARENTS DROP CONSTRAINT BK_BP;

ALTER TABLE BLOCK_PARENTS DROP CONSTRAINT BK_BP2;

ALTER TABLE FILES DROP CONSTRAINT DS_FL;

ALTER TABLE FILES DROP CONSTRAINT BK_FL;

ALTER TABLE FILES DROP CONSTRAINT FT_FL;

ALTER TABLE FILES DROP CONSTRAINT BH_FL;

ALTER TABLE DATASET_OUTPUT_MOD_CONFIGS DROP CONSTRAINT DS_DC;

ALTER TABLE DATASET_OUTPUT_MOD_CONFIGS DROP CONSTRAINT OMC_DC;

ALTER TABLE DATASET_PARENTS DROP CONSTRAINT DS_DP;

ALTER TABLE DATASET_PARENTS DROP CONSTRAINT DS_DP2;

ALTER TABLE DATASET_RUNS DROP CONSTRAINT DS_DR;

ALTER TABLE FILE_OUTPUT_MOD_CONFIGS DROP CONSTRAINT FL_FC;

ALTER TABLE FILE_OUTPUT_MOD_CONFIGS DROP CONSTRAINT OMC_FC;

ALTER TABLE ASSOCIATED_FILES DROP CONSTRAINT FL_AF;

ALTER TABLE ASSOCIATED_FILES DROP CONSTRAINT FL_AF2;

ALTER TABLE FILE_PARENTS DROP CONSTRAINT FL_FP;

ALTER TABLE FILE_PARENTS DROP CONSTRAINT FL_FP2;

ALTER TABLE FILE_LUMIS DROP CONSTRAINT FL_FLM;

ALTER TABLE BLOCK_STORAGE_ELEMENTS DROP CONSTRAINT SE_BSE;

ALTER TABLE BLOCK_STORAGE_ELEMENTS DROP CONSTRAINT BK_BSE;

/* ---------------------------------------------------------------------- */
/* Drop table "APPLICATION_EXECUTABLES"                                   */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE APPLICATION_EXECUTABLES DROP CONSTRAINT NN_AE_APP_EXEC_ID;

ALTER TABLE APPLICATION_EXECUTABLES DROP CONSTRAINT NN_AE_APP_NAME;

ALTER TABLE APPLICATION_EXECUTABLES DROP CONSTRAINT PK_AE;

ALTER TABLE APPLICATION_EXECUTABLES DROP CONSTRAINT TUC_AE_APP_NAME;

/* Drop table */

DROP TABLE APPLICATION_EXECUTABLES;

/* ---------------------------------------------------------------------- */
/* Drop table "RELEASE_VERSIONS"                                          */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE RELEASE_VERSIONS DROP CONSTRAINT NN_RV_RELEASE_VERSION_ID;

ALTER TABLE RELEASE_VERSIONS DROP CONSTRAINT NN_RV_VERSION;

ALTER TABLE RELEASE_VERSIONS DROP CONSTRAINT PK_RV;

ALTER TABLE RELEASE_VERSIONS DROP CONSTRAINT TUC_RV_VERSION;

/* Drop table */

DROP TABLE RELEASE_VERSIONS;

/* ---------------------------------------------------------------------- */
/* Drop table "PROCESSED_DATASETS"                                        */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE PROCESSED_DATASETS DROP CONSTRAINT NN_PSDS_PROCESSED_DS_ID;

ALTER TABLE PROCESSED_DATASETS DROP CONSTRAINT NN_PSDS_PROCESSED_DS_NAME;

ALTER TABLE PROCESSED_DATASETS DROP CONSTRAINT PK_PSDS;

ALTER TABLE PROCESSED_DATASETS DROP CONSTRAINT TUC_PSDS_PROCESSED_DS_NAME;

/* Drop table */

DROP TABLE PROCESSED_DATASETS;

/* ---------------------------------------------------------------------- */
/* Drop table "BRANCH_HASHES"                                             */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE BRANCH_HASHES DROP CONSTRAINT NN_BH_BRANCH_HASH_ID;

ALTER TABLE BRANCH_HASHES DROP CONSTRAINT NN_BH_HASH;

ALTER TABLE BRANCH_HASHES DROP CONSTRAINT PK_BH;

/* Drop table */

DROP TABLE BRANCH_HASHES;

/* ---------------------------------------------------------------------- */
/* Drop table "FILE_TYPES"                                                */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE FILE_TYPES DROP CONSTRAINT NN_FT_FILE_TYPE_ID;

ALTER TABLE FILE_TYPES DROP CONSTRAINT NN_FT_FILE_TYPE;

ALTER TABLE FILE_TYPES DROP CONSTRAINT PK_FT;

ALTER TABLE FILE_TYPES DROP CONSTRAINT TUC_FT_FILE_TYPE;

/* Drop table */

DROP TABLE FILE_TYPES;

/* ---------------------------------------------------------------------- */
/* Drop table "PHYSICS_GROUPS"                                            */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE PHYSICS_GROUPS DROP CONSTRAINT NN_PG_PHYSICS_GROUP_ID;

ALTER TABLE PHYSICS_GROUPS DROP CONSTRAINT NN_PG_PHYSICS_GROUP_NAME;

ALTER TABLE PHYSICS_GROUPS DROP CONSTRAINT PK_PG;

ALTER TABLE PHYSICS_GROUPS DROP CONSTRAINT TUC_PG_PHYSICS_GROUP_NAME;

/* Drop table */

DROP TABLE PHYSICS_GROUPS;

/* ---------------------------------------------------------------------- */
/* Drop table "PRIMARY_DS_TYPES"                                          */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE PRIMARY_DS_TYPES DROP CONSTRAINT NN_PDT_PRIMARY_DS_TYPE_ID;

ALTER TABLE PRIMARY_DS_TYPES DROP CONSTRAINT NN_PDT_PRIMARY_DS_TYPE;

ALTER TABLE PRIMARY_DS_TYPES DROP CONSTRAINT PK_PDT;

ALTER TABLE PRIMARY_DS_TYPES DROP CONSTRAINT TUC_PDT_PRIMARY_DS_TYPE;

/* Drop table */

DROP TABLE PRIMARY_DS_TYPES;

/* ---------------------------------------------------------------------- */
/* Drop table "DATASET_TYPES"                                             */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE DATASET_TYPES DROP CONSTRAINT NN_DTP_DATASET_TYPE_ID;

ALTER TABLE DATASET_TYPES DROP CONSTRAINT NN_DTP_DATASET_TYPE;

ALTER TABLE DATASET_TYPES DROP CONSTRAINT PK_DTP;

ALTER TABLE DATASET_TYPES DROP CONSTRAINT TUC_DTP_DATASET_TYPE;

/* Drop table */

DROP TABLE DATASET_TYPES;

/* ---------------------------------------------------------------------- */
/* Drop table "PARAMETER_SET_HASHES"                                      */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE PARAMETER_SET_HASHES DROP CONSTRAINT NN_PSH_PARAMETER_SET_HASH_ID;

ALTER TABLE PARAMETER_SET_HASHES DROP CONSTRAINT NN_PSH_HASH;

ALTER TABLE PARAMETER_SET_HASHES DROP CONSTRAINT PK_PSH;

ALTER TABLE PARAMETER_SET_HASHES DROP CONSTRAINT TUC_PSH_HASH;

/* Drop table */

DROP TABLE PARAMETER_SET_HASHES;

/* ---------------------------------------------------------------------- */
/* Drop table "DBS_VERSIONS"                                              */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE DBS_VERSIONS DROP CONSTRAINT NN_DV_DBS_VERSION_ID;

ALTER TABLE DBS_VERSIONS DROP CONSTRAINT NN_DV_SCHEMA_VERSION;

ALTER TABLE DBS_VERSIONS DROP CONSTRAINT NN_DV_DBS_RELEASE_VERSION;

ALTER TABLE DBS_VERSIONS DROP CONSTRAINT NN_DV_INSTANCE_NAME;

ALTER TABLE DBS_VERSIONS DROP CONSTRAINT NN_DV_INSTANCE_TYPE;

ALTER TABLE DBS_VERSIONS DROP CONSTRAINT PK_DV;

/* Drop table */

DROP TABLE DBS_VERSIONS;

/* ---------------------------------------------------------------------- */
/* Drop table "SITES"                                                     */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE SITES DROP CONSTRAINT NN_SI_SITE_ID;

ALTER TABLE SITES DROP CONSTRAINT NN_SI_SITE_NAME;

ALTER TABLE SITES DROP CONSTRAINT PK_SI;

ALTER TABLE SITES DROP CONSTRAINT TUC_SI_SITE_NAME;

/* Drop table */

DROP TABLE SITES;

/* ---------------------------------------------------------------------- */
/* Drop table "OUTPUT_MODULE_CONFIGS"                                     */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE OUTPUT_MODULE_CONFIGS DROP CONSTRAINT NN_OMC_OUTPUT_MOD_CONFIG_ID;

ALTER TABLE OUTPUT_MODULE_CONFIGS DROP CONSTRAINT NN_OMC_APP_EXEC_ID;

ALTER TABLE OUTPUT_MODULE_CONFIGS DROP CONSTRAINT NN_OMC_RELEASE_VERSION_ID;

ALTER TABLE OUTPUT_MODULE_CONFIGS DROP CONSTRAINT NN_OMC_PARAMETER_SET_HASH_ID;

ALTER TABLE OUTPUT_MODULE_CONFIGS DROP CONSTRAINT NN_OMC_OUTPUT_MODULE_LABEL;

ALTER TABLE OUTPUT_MODULE_CONFIGS DROP CONSTRAINT PK_OMC;

ALTER TABLE OUTPUT_MODULE_CONFIGS DROP CONSTRAINT TUC_OMC_1;

/* Drop table */

DROP TABLE OUTPUT_MODULE_CONFIGS;

/* ---------------------------------------------------------------------- */
/* Drop table "DATA_TIERS"                                                */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE DATA_TIERS DROP CONSTRAINT NN_DT_DATA_TIER_ID;

ALTER TABLE DATA_TIERS DROP CONSTRAINT NN_DT_DATA_TIER_NAME;

ALTER TABLE DATA_TIERS DROP CONSTRAINT PK_DT;

ALTER TABLE DATA_TIERS DROP CONSTRAINT TUC_DT_DATA_TIER_NAME;

/* Drop table */

DROP TABLE DATA_TIERS;

/* ---------------------------------------------------------------------- */
/* Drop table "PRIMARY_DATASETS"                                          */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE PRIMARY_DATASETS DROP CONSTRAINT NN_PDS_PRIMARY_DS_ID;

ALTER TABLE PRIMARY_DATASETS DROP CONSTRAINT NN_PDS_PRIMARY_DS_NAME;

ALTER TABLE PRIMARY_DATASETS DROP CONSTRAINT NN_PDS_PRIMARY_DS_TYPE_ID;

ALTER TABLE PRIMARY_DATASETS DROP CONSTRAINT PK_PDS;

ALTER TABLE PRIMARY_DATASETS DROP CONSTRAINT TUC_PDS_PRIMARY_DS_NAME;

/* Drop table */

DROP TABLE PRIMARY_DATASETS;

/* ---------------------------------------------------------------------- */
/* Drop table "DATASETS"                                                  */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE DATASETS DROP CONSTRAINT NN_DS_DATASET_ID;

ALTER TABLE DATASETS DROP CONSTRAINT NN_DS_DATA_PATH;

ALTER TABLE DATASETS DROP CONSTRAINT NN_DS_IS_PATH_VALID;

ALTER TABLE DATASETS DROP CONSTRAINT CC_DS_IS_PATH_VALID;

ALTER TABLE DATASETS DROP CONSTRAINT NN_DS_PRIMARY_DS_ID;

ALTER TABLE DATASETS DROP CONSTRAINT NN_DS_PROCESSED_DS_ID;

ALTER TABLE DATASETS DROP CONSTRAINT NN_DS_DATA_TIER_ID;

ALTER TABLE DATASETS DROP CONSTRAINT NN_DS_PATH_TYPE_ID;

ALTER TABLE DATASETS DROP CONSTRAINT PK_DS;

ALTER TABLE DATASETS DROP CONSTRAINT TUC_DS_DATA_PATH;

/* Drop table */

DROP TABLE DATASETS;

/* ---------------------------------------------------------------------- */
/* Drop table "BLOCKS"                                                    */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE BLOCKS DROP CONSTRAINT NN_BK_BLOCK_ID;

ALTER TABLE BLOCKS DROP CONSTRAINT NN_BK_BLOCK_NAME;

ALTER TABLE BLOCKS DROP CONSTRAINT NN_BK_DATASET_ID;

ALTER TABLE BLOCKS DROP CONSTRAINT NN_BK_OPEN_FOR_WRITING;

ALTER TABLE BLOCKS DROP CONSTRAINT CC_BK_OPEN_FOR_WRITING;

ALTER TABLE BLOCKS DROP CONSTRAINT PK_BK;

ALTER TABLE BLOCKS DROP CONSTRAINT TUC_BK_BLOCK_NAME;

/* Drop table */

DROP TABLE BLOCKS;

/* ---------------------------------------------------------------------- */
/* Drop table "BLOCK_PARENTS"                                             */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE BLOCK_PARENTS DROP CONSTRAINT NN_BP_BLOCK_PARENT_ID;

ALTER TABLE BLOCK_PARENTS DROP CONSTRAINT NN_BP_THIS_BLOCK_ID;

ALTER TABLE BLOCK_PARENTS DROP CONSTRAINT NN_BP_PARENT_BLOCK_ID;

ALTER TABLE BLOCK_PARENTS DROP CONSTRAINT PK_BP;

/* Drop table */

DROP TABLE BLOCK_PARENTS;

/* ---------------------------------------------------------------------- */
/* Drop table "FILES"                                                     */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE FILES DROP CONSTRAINT NN_FL_FILE_ID;

ALTER TABLE FILES DROP CONSTRAINT NN_FL_LOGICAL_FILE_NAME;

ALTER TABLE FILES DROP CONSTRAINT NN_FL_IS_FILE_VALID;

ALTER TABLE FILES DROP CONSTRAINT CC_FL_IS_FILE_VALID;

ALTER TABLE FILES DROP CONSTRAINT NN_FL_PATH_ID;

ALTER TABLE FILES DROP CONSTRAINT NN_FL_BLOCK_ID;

ALTER TABLE FILES DROP CONSTRAINT NN_FL_FILE_TYPE_ID;

ALTER TABLE FILES DROP CONSTRAINT NN_FL_CHECK_SUM;

ALTER TABLE FILES DROP CONSTRAINT NN_FL_EVENT_COUNT;

ALTER TABLE FILES DROP CONSTRAINT NN_FL_FILE_SIZE;

ALTER TABLE FILES DROP CONSTRAINT PK_FL;

ALTER TABLE FILES DROP CONSTRAINT TUC_FL_LOGICAL_FILE_NAME;

/* Drop table */

DROP TABLE FILES;

/* ---------------------------------------------------------------------- */
/* Drop table "DATASET_OUTPUT_MOD_CONFIGS"                                */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE DATASET_OUTPUT_MOD_CONFIGS DROP CONSTRAINT NN_DC_DS_OUTPUT_MOD_CONF_ID;

ALTER TABLE DATASET_OUTPUT_MOD_CONFIGS DROP CONSTRAINT NN_DC_DATASET_ID;

ALTER TABLE DATASET_OUTPUT_MOD_CONFIGS DROP CONSTRAINT NN_DC_OUTPUT_MOD_CONFIG_ID;

ALTER TABLE DATASET_OUTPUT_MOD_CONFIGS DROP CONSTRAINT PK_DC;

/* Drop table */

DROP TABLE DATASET_OUTPUT_MOD_CONFIGS;

/* ---------------------------------------------------------------------- */
/* Drop table "DATASET_PARENTS"                                           */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE DATASET_PARENTS DROP CONSTRAINT NN_DP_DATASET_PARENT_ID;

ALTER TABLE DATASET_PARENTS DROP CONSTRAINT NN_DP_THIS_DATASET_ID;

ALTER TABLE DATASET_PARENTS DROP CONSTRAINT NN_DP_PARENT_DATASET_ID;

ALTER TABLE DATASET_PARENTS DROP CONSTRAINT PK_DP;

/* Drop table */

DROP TABLE DATASET_PARENTS;

/* ---------------------------------------------------------------------- */
/* Drop table "DATASET_RUNS"                                              */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE DATASET_RUNS DROP CONSTRAINT NN_DR_PATH_RUN_ID;

ALTER TABLE DATASET_RUNS DROP CONSTRAINT NN_DR_DATASET_ID;

ALTER TABLE DATASET_RUNS DROP CONSTRAINT PK_DR;

/* Drop table */

DROP TABLE DATASET_RUNS;

/* ---------------------------------------------------------------------- */
/* Drop table "FILE_OUTPUT_MOD_CONFIGS"                                   */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE FILE_OUTPUT_MOD_CONFIGS DROP CONSTRAINT NN_FC_FILE_OUTPUT_CONFIG_ID;

ALTER TABLE FILE_OUTPUT_MOD_CONFIGS DROP CONSTRAINT NN_FC_FILE_ID;

ALTER TABLE FILE_OUTPUT_MOD_CONFIGS DROP CONSTRAINT NN_FC_OUTPUT_MOD_CONFIG_ID;

ALTER TABLE FILE_OUTPUT_MOD_CONFIGS DROP CONSTRAINT PK_FC;

/* Drop table */

DROP TABLE FILE_OUTPUT_MOD_CONFIGS;

/* ---------------------------------------------------------------------- */
/* Drop table "ASSOCIATED_FILES"                                          */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE ASSOCIATED_FILES DROP CONSTRAINT NN_AF_ASSOCATED_FILE_ID;

ALTER TABLE ASSOCIATED_FILES DROP CONSTRAINT NN_AF_THIS_FILE_ID;

ALTER TABLE ASSOCIATED_FILES DROP CONSTRAINT NN_AF_ASSOCATED_FILE;

ALTER TABLE ASSOCIATED_FILES DROP CONSTRAINT PK_AF;

/* Drop table */

DROP TABLE ASSOCIATED_FILES;

/* ---------------------------------------------------------------------- */
/* Drop table "FILE_PARENTS"                                              */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE FILE_PARENTS DROP CONSTRAINT NN_FP_FILE_PARENT_ID;

ALTER TABLE FILE_PARENTS DROP CONSTRAINT NN_FP_THIS_FILE_ID;

ALTER TABLE FILE_PARENTS DROP CONSTRAINT NN_FP_PARENT_FILE_ID;

ALTER TABLE FILE_PARENTS DROP CONSTRAINT PK_FP;

/* Drop table */

DROP TABLE FILE_PARENTS;

/* ---------------------------------------------------------------------- */
/* Drop table "FILE_LUMIS"                                                */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE FILE_LUMIS DROP CONSTRAINT NN_FLM_FILE_LUMI_ID;

ALTER TABLE FILE_LUMIS DROP CONSTRAINT NN_FLM_RUN_NUM;

ALTER TABLE FILE_LUMIS DROP CONSTRAINT NN_FLM_LUMI_SECTION_NUM;

ALTER TABLE FILE_LUMIS DROP CONSTRAINT NN_FLM_FILE_ID;

ALTER TABLE FILE_LUMIS DROP CONSTRAINT PK_FLM;

/* Drop table */

DROP TABLE FILE_LUMIS;

/* ---------------------------------------------------------------------- */
/* Drop table "ACQUISITION_ERAS"                                          */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE ACQUISITION_ERAS DROP CONSTRAINT NN_AQE_ACQUISITION_ERA_ID;

ALTER TABLE ACQUISITION_ERAS DROP CONSTRAINT NN_AQE_ACQUISITION_ERA_NAME;

ALTER TABLE ACQUISITION_ERAS DROP CONSTRAINT PK_AQE;

ALTER TABLE ACQUISITION_ERAS DROP CONSTRAINT TUC_AQE_ACQUISITION_ERA_NAME;

/* Drop table */

DROP TABLE ACQUISITION_ERAS;

/* ---------------------------------------------------------------------- */
/* Drop table "PROCESSING_ERAS"                                           */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE PROCESSING_ERAS DROP CONSTRAINT NN_PE_PROCESSING_ERA_ID;

ALTER TABLE PROCESSING_ERAS DROP CONSTRAINT PK_PE;

ALTER TABLE PROCESSING_ERAS DROP CONSTRAINT TUC_PE_PROCESSING_VERSION;

/* Drop table */

DROP TABLE PROCESSING_ERAS;

/* ---------------------------------------------------------------------- */
/* Drop table "STORAGE_ELEMENTS"                                          */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE STORAGE_ELEMENTS DROP CONSTRAINT NN_SE_SE_ID;

ALTER TABLE STORAGE_ELEMENTS DROP CONSTRAINT PK_SE;

ALTER TABLE STORAGE_ELEMENTS DROP CONSTRAINT TUC_SE_SE_NAME;

/* Drop table */

DROP TABLE STORAGE_ELEMENTS;

/* ---------------------------------------------------------------------- */
/* Drop table "BLOCK_STORAGE_ELEMENTS"                                    */
/* ---------------------------------------------------------------------- */

/* Drop constraints */

ALTER TABLE BLOCK_STORAGE_ELEMENTS DROP CONSTRAINT NN_BSE_BLOCK_SE_ID;

ALTER TABLE BLOCK_STORAGE_ELEMENTS DROP CONSTRAINT NN_BSE_SE_ID;

ALTER TABLE BLOCK_STORAGE_ELEMENTS DROP CONSTRAINT NN_BSE_BLOCK_ID;

ALTER TABLE BLOCK_STORAGE_ELEMENTS DROP CONSTRAINT PK_BSE;

/* Drop table */

DROP TABLE BLOCK_STORAGE_ELEMENTS;

/* ---------------------------------------------------------------------- */
/* Drop sequences                                                         */
/* ---------------------------------------------------------------------- */

DROP SEQUENCE SEQ_RV;

DROP SEQUENCE SEQ_PSH;

DROP SEQUENCE SEQ_DR;

DROP SEQUENCE SEQ_PG;

DROP SEQUENCE SEQ_DT;

DROP SEQUENCE SEQ_PDS;

DROP SEQUENCE SEQ_PDT;

DROP SEQUENCE SEQ_OMC;

DROP SEQUENCE SEQ_DC;

DROP SEQUENCE SEQ_DP;

DROP SEQUENCE SEQ_DTP;

DROP SEQUENCE SEQ_DS;

DROP SEQUENCE SEQ_AE;

DROP SEQUENCE SEQ_BP;

DROP SEQUENCE SEQ_PSDS;

DROP SEQUENCE SEQ_AQE;

DROP SEQUENCE SEQ_PE;

DROP SEQUENCE SEQ_FLM;

DROP SEQUENCE SEQ_BK;

DROP SEQUENCE SEQ_SI;

DROP SEQUENCE SEQ_SE;

DROP SEQUENCE SEQ_BSE;

DROP SEQUENCE SEQ_FL;

DROP SEQUENCE SEQ_FP;

DROP SEQUENCE SEQ_FT;

DROP SEQUENCE SEQ_AF;

DROP SEQUENCE SEQ_BH;

DROP SEQUENCE SEQ_FC;

DROP SEQUENCE SEQ_DV;

DROP ROLE CMS_DBS3_READ_ROLE;
DROP ROLE CMS_DBS3_WRITE_ROLE;
DROP ROLE CMS_DBS3_ADMIN_ROLE;
