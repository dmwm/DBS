# ---------------------------------------------------------------------- #
# Script generated with: DeZign for Databases v5.2.3                     #
# Target DBMS:           MySQL 5                                         #
# Project file:          DBS3.dez                                        #
# Project name:          DBS3                                            #
# Author:                Yuyi Guo for DBS Group                          #
# Script type:           Database drop script                            #
# Created on:            2009-09-10 09:42                                #
# Model version:         Version 2009-09-10                              #
# ---------------------------------------------------------------------- #


# ---------------------------------------------------------------------- #
# Drop foreign key constraints                                           #
# ---------------------------------------------------------------------- #

ALTER TABLE `PROCESS_CONFIGURATIONS` DROP FOREIGN KEY `AE_PC`;

ALTER TABLE `PROCESS_CONFIGURATIONS` DROP FOREIGN KEY `RV_PC`;

ALTER TABLE `PROCESS_CONFIGURATIONS` DROP FOREIGN KEY `PSH_PC`;

ALTER TABLE `PRIMARY_DATASETS` DROP FOREIGN KEY `PDT_PDS`;

ALTER TABLE `PATHS` DROP FOREIGN KEY `PDS_PH`;

ALTER TABLE `PATHS` DROP FOREIGN KEY `DT_PH`;

ALTER TABLE `PATHS` DROP FOREIGN KEY `PSDS_PH`;

ALTER TABLE `PATHS` DROP FOREIGN KEY `PT_PH`;

ALTER TABLE `PATHS` DROP FOREIGN KEY `PG_PH`;

ALTER TABLE `PATHS` DROP FOREIGN KEY `AQE_PH`;

ALTER TABLE `PATHS` DROP FOREIGN KEY `PE_PH`;

ALTER TABLE `BLOCKS` DROP FOREIGN KEY `PH_BK`;

ALTER TABLE `BLOCKS` DROP FOREIGN KEY `SI_BK`;

ALTER TABLE `BLOCK_PARENTS` DROP FOREIGN KEY `BK_BP`;

ALTER TABLE `BLOCK_PARENTS` DROP FOREIGN KEY `BK_BP2`;

ALTER TABLE `FILES` DROP FOREIGN KEY `PH_FL`;

ALTER TABLE `FILES` DROP FOREIGN KEY `BK_FL`;

ALTER TABLE `FILES` DROP FOREIGN KEY `FT_FL`;

ALTER TABLE `FILES` DROP FOREIGN KEY `BH_FL`;

ALTER TABLE `PATH_PROCESS_CONFIGS` DROP FOREIGN KEY `PH_PPC`;

ALTER TABLE `PATH_PROCESS_CONFIGS` DROP FOREIGN KEY `PC_PPC`;

ALTER TABLE `PATH_PARENTS` DROP FOREIGN KEY `PH_PP`;

ALTER TABLE `PATH_PARENTS` DROP FOREIGN KEY `PH_PP2`;

ALTER TABLE `PATH_RUNS` DROP FOREIGN KEY `PH_PR`;

ALTER TABLE `FILE_PROCESS_CONFIGS` DROP FOREIGN KEY `FL_FPC`;

ALTER TABLE `FILE_PROCESS_CONFIGS` DROP FOREIGN KEY `PC_FPC`;

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

DROP INDEX `TUC_RV_VERSION` ON `RELEASE_VERSIONS`;

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
# Drop table "PATH_TYPES"                                                #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `PATH_TYPES` DROP PRIMARY KEY;

DROP INDEX `TUC_PT_PATH_TYPE` ON `PATH_TYPES`;

# Drop table #

DROP TABLE `PATH_TYPES`;

# ---------------------------------------------------------------------- #
# Drop table "PARAMETER_SET_HASHES"                                      #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `PARAMETER_SET_HASHES` DROP PRIMARY KEY;

DROP INDEX `TUC_PSH_HASH` ON `PARAMETER_SET_HASHES`;

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
# Drop table "PROCESS_CONFIGURATIONS"                                    #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `PROCESS_CONFIGURATIONS` ALTER COLUMN `OUTPUT_MODULE_LABEL` DROP DEFAULT;

ALTER TABLE `PROCESS_CONFIGURATIONS` DROP PRIMARY KEY;

DROP INDEX `TUC_PC_1` ON `PROCESS_CONFIGURATIONS`;

# Drop table #

DROP TABLE `PROCESS_CONFIGURATIONS`;

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
# Drop table "PATHS"                                                     #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `PATHS` ALTER COLUMN `IS_PATH_VALID` DROP DEFAULT;

ALTER TABLE `PATHS` DROP PRIMARY KEY;

DROP INDEX `TUC_PH_PATH_NAME` ON `PATHS`;

# Drop table #

DROP TABLE `PATHS`;

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
# Drop table "PATH_PROCESS_CONFIGS"                                      #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `PATH_PROCESS_CONFIGS` DROP PRIMARY KEY;

# Drop table #

DROP TABLE `PATH_PROCESS_CONFIGS`;

# ---------------------------------------------------------------------- #
# Drop table "PATH_PARENTS"                                              #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `PATH_PARENTS` DROP PRIMARY KEY;

# Drop table #

DROP TABLE `PATH_PARENTS`;

# ---------------------------------------------------------------------- #
# Drop table "PATH_RUNS"                                                 #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `PATH_RUNS` ALTER COLUMN `COMPLETE` DROP DEFAULT;

ALTER TABLE `PATH_RUNS` DROP PRIMARY KEY;

# Drop table #

DROP TABLE `PATH_RUNS`;

# ---------------------------------------------------------------------- #
# Drop table "FILE_PROCESS_CONFIGS"                                      #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `FILE_PROCESS_CONFIGS` DROP PRIMARY KEY;

# Drop table #

DROP TABLE `FILE_PROCESS_CONFIGS`;

# ---------------------------------------------------------------------- #
# Drop table "ASSOCIATED_FILES"                                          #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `ASSOCIATED_FILES` DROP PRIMARY KEY;

# Drop table #

DROP TABLE `ASSOCIATED_FILES`;

# ---------------------------------------------------------------------- #
# Drop table "FILE_PARENTS"                                              #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `FILE_PARENTS` DROP PRIMARY KEY;

# Drop table #

DROP TABLE `FILE_PARENTS`;

# ---------------------------------------------------------------------- #
# Drop table "FILE_LUMIS"                                                #
# ---------------------------------------------------------------------- #

# Drop constraints #

ALTER TABLE `FILE_LUMIS` DROP PRIMARY KEY;

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

DROP INDEX `TUC_PE_PROCESSING_ERA_NAME` ON `PROCESSING_ERAS`;

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

# Drop table #

DROP TABLE `BLOCK_STORAGE_ELEMENTS`;

DROP ROLE CMS_DBS3_READ_ROLE;
DROP ROLE CMS_DBS3_WRITE_ROLE;
DROP ROLE CMS_DBS3_ADMIN_ROLE;
