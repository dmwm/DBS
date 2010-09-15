# ---------------------------------------------------------------------- #
# Script generated with: DeZign for Databases v5.2.3                     #
# Target DBMS:           MySQL 5                                         #
# Project file:          DBS3.dez                                        #
# Project name:          DBS3                                            #
# Author:                Yuyi Guo for DBS Group                          #
# Script type:           Database drop script                            #
# Created on:            2010-02-08 10:21                                #
# Model version:         Version 2010-02-08                              #
# ---------------------------------------------------------------------- #


# ---------------------------------------------------------------------- #
# Drop foreign key constraints                                           #
# ---------------------------------------------------------------------- #

ALTER TABLE `OUTPUT_MODULE_CONFIGS` DROP FOREIGN KEY `AE_OMC`;

ALTER TABLE `OUTPUT_MODULE_CONFIGS` DROP FOREIGN KEY `RV_OMC`;

ALTER TABLE `OUTPUT_MODULE_CONFIGS` DROP FOREIGN KEY `PSH_OMC`;

ALTER TABLE `PRIMARY_DATASETS` DROP FOREIGN KEY `PDT_PDS`;

ALTER TABLE `DATASETS` DROP FOREIGN KEY `PDS_DS`;

ALTER TABLE `DATASETS` DROP FOREIGN KEY `DT_DS`;

ALTER TABLE `DATASETS` DROP FOREIGN KEY `PSDS_DS`;

ALTER TABLE `DATASETS` DROP FOREIGN KEY `DTP_DS`;

ALTER TABLE `DATASETS` DROP FOREIGN KEY `PG_DS`;

ALTER TABLE `DATASETS` DROP FOREIGN KEY `AQE_DS`;

ALTER TABLE `DATASETS` DROP FOREIGN KEY `PE_DS`;

ALTER TABLE `BLOCKS` DROP FOREIGN KEY `DS_BK`;

ALTER TABLE `BLOCKS` DROP FOREIGN KEY `SI_BK`;

ALTER TABLE `BLOCK_PARENTS` DROP FOREIGN KEY `BK_BP`;

ALTER TABLE `BLOCK_PARENTS` DROP FOREIGN KEY `BK_BP2`;

ALTER TABLE `FILES` DROP FOREIGN KEY `DS_FL`;

ALTER TABLE `FILES` DROP FOREIGN KEY `BK_FL`;

ALTER TABLE `FILES` DROP FOREIGN KEY `FT_FL`;

ALTER TABLE `FILES` DROP FOREIGN KEY `BH_FL`;

ALTER TABLE `DATASET_OUTPUT_MOD_CONFIGS` DROP FOREIGN KEY `DS_DC`;

ALTER TABLE `DATASET_OUTPUT_MOD_CONFIGS` DROP FOREIGN KEY `OMC_DC`;

ALTER TABLE `DATASET_PARENTS` DROP FOREIGN KEY `DS_DP`;

ALTER TABLE `DATASET_PARENTS` DROP FOREIGN KEY `DS_DP2`;

ALTER TABLE `DATASET_RUNS` DROP FOREIGN KEY `DS_DR`;

ALTER TABLE `FILE_OUTPUT_MOD_CONFIGS` DROP FOREIGN KEY `FL_FC`;

ALTER TABLE `FILE_OUTPUT_MOD_CONFIGS` DROP FOREIGN KEY `OMC_FC`;

ALTER TABLE `ASSOCIATED_FILES` DROP FOREIGN KEY `FL_AF`;

ALTER TABLE `ASSOCIATED_FILES` DROP FOREIGN KEY `FL_AF2`;

ALTER TABLE `FILE_PARENTS` DROP FOREIGN KEY `FL_FP`;

ALTER TABLE `FILE_PARENTS` DROP FOREIGN KEY `FL_FP2`;

ALTER TABLE `FILE_LUMIS` DROP FOREIGN KEY `FL_FLM`;

ALTER TABLE `BLOCK_STORAGE_ELEMENTS` DROP FOREIGN KEY `SE_BSE`;

ALTER TABLE `BLOCK_STORAGE_ELEMENTS` DROP FOREIGN KEY `BK_BSE`;

# ---------------------------------------------------------------------- #
# Drop table "APPLICATION_EXECUTABLES"                                   #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `APPLICATION_EXECUTABLES` DROP PRIMARY KEY;

DROP INDEX `TUC_AE_APP_NAME` ON `APPLICATION_EXECUTABLES`;

# Drop table #

DROP TABLE `APPLICATION_EXECUTABLES`;

# ---------------------------------------------------------------------- #
# Drop table "RELEASE_VERSIONS"                                          #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `RELEASE_VERSIONS` DROP PRIMARY KEY;

DROP INDEX `TUC_RV_RELEASE_VERSION` ON `RELEASE_VERSIONS`;

# Drop table #

DROP TABLE `RELEASE_VERSIONS`;

# ---------------------------------------------------------------------- #
# Drop table "PROCESSED_DATASETS"                                        #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `PROCESSED_DATASETS` DROP PRIMARY KEY;

DROP INDEX `TUC_PSDS_PROCESSED_DS_NAME` ON `PROCESSED_DATASETS`;

# Drop table #

DROP TABLE `PROCESSED_DATASETS`;

# ---------------------------------------------------------------------- #
# Drop table "BRANCH_HASHES"                                             #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `BRANCH_HASHES` DROP PRIMARY KEY;

# Drop table #

DROP TABLE `BRANCH_HASHES`;

# ---------------------------------------------------------------------- #
# Drop table "FILE_TYPES"                                                #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `FILE_TYPES` DROP PRIMARY KEY;

DROP INDEX `TUC_FT_FILE_TYPE` ON `FILE_TYPES`;

# Drop table #

DROP TABLE `FILE_TYPES`;

# ---------------------------------------------------------------------- #
# Drop table "PHYSICS_GROUPS"                                            #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `PHYSICS_GROUPS` DROP PRIMARY KEY;

DROP INDEX `TUC_PG_PHYSICS_GROUP_NAME` ON `PHYSICS_GROUPS`;

# Drop table #

DROP TABLE `PHYSICS_GROUPS`;

# ---------------------------------------------------------------------- #
# Drop table "PRIMARY_DS_TYPES"                                          #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `PRIMARY_DS_TYPES` DROP PRIMARY KEY;

DROP INDEX `TUC_PDT_PRIMARY_DS_TYPE` ON `PRIMARY_DS_TYPES`;

# Drop table #

DROP TABLE `PRIMARY_DS_TYPES`;

# ---------------------------------------------------------------------- #
# Drop table "DATASET_TYPES"                                             #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `DATASET_TYPES` DROP PRIMARY KEY;

DROP INDEX `TUC_DTP_DATASET_TYPE` ON `DATASET_TYPES`;

# Drop table #

DROP TABLE `DATASET_TYPES`;

# ---------------------------------------------------------------------- #
# Drop table "PARAMETER_SET_HASHES"                                      #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `PARAMETER_SET_HASHES` DROP PRIMARY KEY;

DROP INDEX `TUC_PSH_PSET_HASH` ON `PARAMETER_SET_HASHES`;

# Drop table #

DROP TABLE `PARAMETER_SET_HASHES`;

# ---------------------------------------------------------------------- #
# Drop table "DBS_VERSIONS"                                              #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `DBS_VERSIONS` DROP PRIMARY KEY;

# Drop table #

DROP TABLE `DBS_VERSIONS`;

# ---------------------------------------------------------------------- #
# Drop table "SITES"                                                     #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `SITES` DROP PRIMARY KEY;

DROP INDEX `TUC_SI_SITE_NAME` ON `SITES`;

# Drop table #

DROP TABLE `SITES`;

# ---------------------------------------------------------------------- #
# Drop table "OUTPUT_MODULE_CONFIGS"                                     #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `OUTPUT_MODULE_CONFIGS` ALTER COLUMN `OUTPUT_MODULE_LABEL` DROP DEFAULT;

ALTER TABLE `OUTPUT_MODULE_CONFIGS` DROP PRIMARY KEY;

DROP INDEX `TUC_OMC_1` ON `OUTPUT_MODULE_CONFIGS`;

# Drop table #

DROP TABLE `OUTPUT_MODULE_CONFIGS`;

# ---------------------------------------------------------------------- #
# Drop table "DATA_TIERS"                                                #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `DATA_TIERS` DROP PRIMARY KEY;

DROP INDEX `TUC_DT_DATA_TIER_NAME` ON `DATA_TIERS`;

# Drop table #

DROP TABLE `DATA_TIERS`;

# ---------------------------------------------------------------------- #
# Drop table "PRIMARY_DATASETS"                                          #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `PRIMARY_DATASETS` DROP PRIMARY KEY;

DROP INDEX `TUC_PDS_PRIMARY_DS_NAME` ON `PRIMARY_DATASETS`;

# Drop table #

DROP TABLE `PRIMARY_DATASETS`;

# ---------------------------------------------------------------------- #
# Drop table "DATASETS"                                                  #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `DATASETS` ALTER COLUMN `IS_DATASET_VALID` DROP DEFAULT;

ALTER TABLE `DATASETS` DROP PRIMARY KEY;

DROP INDEX `TUC_DS_DATASET` ON `DATASETS`;

# Drop table #

DROP TABLE `DATASETS`;

# ---------------------------------------------------------------------- #
# Drop table "BLOCKS"                                                    #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `BLOCKS` ALTER COLUMN `OPEN_FOR_WRITING` DROP DEFAULT;

ALTER TABLE `BLOCKS` DROP PRIMARY KEY;

DROP INDEX `TUC_BK_BLOCK_NAME` ON `BLOCKS`;

# Drop table #

DROP TABLE `BLOCKS`;

# ---------------------------------------------------------------------- #
# Drop table "BLOCK_PARENTS"                                             #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `BLOCK_PARENTS` DROP PRIMARY KEY;

DROP INDEX `TUC_BP_1` ON `BLOCK_PARENTS`;

# Drop table #

DROP TABLE `BLOCK_PARENTS`;

# ---------------------------------------------------------------------- #
# Drop table "FILES"                                                     #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `FILES` ALTER COLUMN `IS_FILE_VALID` DROP DEFAULT;

ALTER TABLE `FILES` ALTER COLUMN `ADLER32` DROP DEFAULT;

ALTER TABLE `FILES` ALTER COLUMN `MD5` DROP DEFAULT;

ALTER TABLE `FILES` DROP PRIMARY KEY;

DROP INDEX `TUC_FL_LOGICAL_FILE_NAME` ON `FILES`;

# Drop table #

DROP TABLE `FILES`;

# ---------------------------------------------------------------------- #
# Drop table "DATASET_OUTPUT_MOD_CONFIGS"                                #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `DATASET_OUTPUT_MOD_CONFIGS` DROP PRIMARY KEY;

DROP INDEX `TUC_DC_1` ON `DATASET_OUTPUT_MOD_CONFIGS`;

# Drop table #

DROP TABLE `DATASET_OUTPUT_MOD_CONFIGS`;

# ---------------------------------------------------------------------- #
# Drop table "DATASET_PARENTS"                                           #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `DATASET_PARENTS` DROP PRIMARY KEY;

DROP INDEX `TUC_DP_1` ON `DATASET_PARENTS`;

# Drop table #

DROP TABLE `DATASET_PARENTS`;

# ---------------------------------------------------------------------- #
# Drop table "DATASET_RUNS"                                              #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `DATASET_RUNS` ALTER COLUMN `COMPLETE` DROP DEFAULT;

ALTER TABLE `DATASET_RUNS` DROP PRIMARY KEY;

# Drop table #

DROP TABLE `DATASET_RUNS`;

# ---------------------------------------------------------------------- #
# Drop table "FILE_OUTPUT_MOD_CONFIGS"                                   #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `FILE_OUTPUT_MOD_CONFIGS` DROP PRIMARY KEY;

DROP INDEX `TUC_FC_1` ON `FILE_OUTPUT_MOD_CONFIGS`;

# Drop table #

DROP TABLE `FILE_OUTPUT_MOD_CONFIGS`;

# ---------------------------------------------------------------------- #
# Drop table "ASSOCIATED_FILES"                                          #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `ASSOCIATED_FILES` DROP PRIMARY KEY;

DROP INDEX `TUC_AF_1` ON `ASSOCIATED_FILES`;

# Drop table #

DROP TABLE `ASSOCIATED_FILES`;

# ---------------------------------------------------------------------- #
# Drop table "FILE_PARENTS"                                              #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `FILE_PARENTS` DROP PRIMARY KEY;

DROP INDEX `TUC_FP_1` ON `FILE_PARENTS`;

# Drop table #

DROP TABLE `FILE_PARENTS`;

# ---------------------------------------------------------------------- #
# Drop table "FILE_LUMIS"                                                #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `FILE_LUMIS` DROP PRIMARY KEY;

DROP INDEX `TUC_FLM_1` ON `FILE_LUMIS`;

# Drop table #

DROP TABLE `FILE_LUMIS`;

# ---------------------------------------------------------------------- #
# Drop table "ACQUISITION_ERAS"                                          #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `ACQUISITION_ERAS` DROP PRIMARY KEY;

DROP INDEX `TUC_AQE_ACQUISITION_ERA_NAME` ON `ACQUISITION_ERAS`;

# Drop table #

DROP TABLE `ACQUISITION_ERAS`;

# ---------------------------------------------------------------------- #
# Drop table "PROCESSING_ERAS"                                           #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `PROCESSING_ERAS` DROP PRIMARY KEY;

DROP INDEX `TUC_PE_PROCESSING_VERSION` ON `PROCESSING_ERAS`;

# Drop table #

DROP TABLE `PROCESSING_ERAS`;

# ---------------------------------------------------------------------- #
# Drop table "STORAGE_ELEMENTS"                                          #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `STORAGE_ELEMENTS` DROP PRIMARY KEY;

DROP INDEX `TUC_SE_SE_NAME` ON `STORAGE_ELEMENTS`;

# Drop table #

DROP TABLE `STORAGE_ELEMENTS`;

# ---------------------------------------------------------------------- #
# Drop table "BLOCK_STORAGE_ELEMENTS"                                    #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `BLOCK_STORAGE_ELEMENTS` DROP PRIMARY KEY;

DROP INDEX `TUC_BSE_1` ON `BLOCK_STORAGE_ELEMENTS`;

# Drop table #

DROP TABLE `BLOCK_STORAGE_ELEMENTS`;

DROP ROLE CMS_DBS3_READ_ROLE;
DROP ROLE CMS_DBS3_WRITE_ROLE;
DROP ROLE CMS_DBS3_ADMIN_ROLE;
