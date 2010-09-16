# ---------------------------------------------------------------------- #
# Script generated with: DeZign for Databases v6.1.2                     #
# Target DBMS:           MySQL 5                                         #
# Project file:          DBS3.dez                                        #
# Project name:          DBS3                                            #
# Author:                Yuyi Guo for DBS Group                          #
# Script type:           Database creation script                        #
# Created on:            2010-08-31 14:30                                #
# ---------------------------------------------------------------------- #


drop database if exists CMS_DBS3;
create database CMS_DBS3;
use CMS_DBS3;

# ---------------------------------------------------------------------- #
# Tables                                                                 #
# ---------------------------------------------------------------------- #

# ---------------------------------------------------------------------- #
# Add table "APPLICATION_EXECUTABLES"                                    #
# ---------------------------------------------------------------------- #

CREATE TABLE `APPLICATION_EXECUTABLES` (
    `APP_EXEC_ID` INTEGER NOT NULL,
    `APP_NAME` VARCHAR(100) NOT NULL,
    CONSTRAINT `PK_AE` PRIMARY KEY (`APP_EXEC_ID`),
    CONSTRAINT `TUC_AE_APP_NAME` UNIQUE (`APP_NAME`)
)
ENGINE = InnoDB ;

# ---------------------------------------------------------------------- #
# Add table "RELEASE_VERSIONS"                                           #
# ---------------------------------------------------------------------- #

CREATE TABLE `RELEASE_VERSIONS` (
    `RELEASE_VERSION_ID` INTEGER NOT NULL,
    `RELEASE_VERSION` VARCHAR(100) NOT NULL,
    CONSTRAINT `PK_RV` PRIMARY KEY (`RELEASE_VERSION_ID`),
    CONSTRAINT `TUC_RV_RELEASE_VERSION` UNIQUE (`RELEASE_VERSION`)
)
ENGINE = InnoDB ;

# ---------------------------------------------------------------------- #
# Add table "PROCESSED_DATASETS"                                         #
# ---------------------------------------------------------------------- #

CREATE TABLE `PROCESSED_DATASETS` (
    `PROCESSED_DS_ID` INTEGER NOT NULL,
    `PROCESSED_DS_NAME` VARCHAR(100) NOT NULL,
    CONSTRAINT `PK_PSDS` PRIMARY KEY (`PROCESSED_DS_ID`),
    CONSTRAINT `TUC_PSDS_PROCESSED_DS_NAME` UNIQUE (`PROCESSED_DS_NAME`)
)
ENGINE = InnoDB ;

# ---------------------------------------------------------------------- #
# Add table "BRANCH_HASHES"                                              #
# ---------------------------------------------------------------------- #

CREATE TABLE `BRANCH_HASHES` (
    `BRANCH_HASH_ID` INTEGER NOT NULL,
    `BRANCH_HASH` VARCHAR(700) NOT NULL,
    `CONTENT` MEDIUMTEXT,
    CONSTRAINT `PK_BH` PRIMARY KEY (`BRANCH_HASH_ID`)
)
ENGINE = InnoDB ;

# ---------------------------------------------------------------------- #
# Add table "FILE_DATA_TYPES"                                            #
# ---------------------------------------------------------------------- #

CREATE TABLE `FILE_DATA_TYPES` (
    `FILE_TYPE_ID` INTEGER NOT NULL,
    `FILE_TYPE` VARCHAR(100) NOT NULL,
    CONSTRAINT `PK_FT` PRIMARY KEY (`FILE_TYPE_ID`),
    CONSTRAINT `TUC_FT_FILE_TYPE` UNIQUE (`FILE_TYPE`)
)
ENGINE = InnoDB ;

# ---------------------------------------------------------------------- #
# Add table "PHYSICS_GROUPS"                                             #
# ---------------------------------------------------------------------- #

CREATE TABLE `PHYSICS_GROUPS` (
    `PHYSICS_GROUP_ID` INTEGER NOT NULL,
    `PHYSICS_GROUP_NAME` VARCHAR(100) NOT NULL,
    `PHYSICS_GROUP_CONVENER` VARCHAR(100),
    CONSTRAINT `PK_PG` PRIMARY KEY (`PHYSICS_GROUP_ID`),
    CONSTRAINT `TUC_PG_PHYSICS_GROUP_NAME` UNIQUE (`PHYSICS_GROUP_NAME`)
)
ENGINE = InnoDB ;

# ---------------------------------------------------------------------- #
# Add table "PRIMARY_DS_TYPES"                                           #
# ---------------------------------------------------------------------- #

CREATE TABLE `PRIMARY_DS_TYPES` (
    `PRIMARY_DS_TYPE_ID` INTEGER NOT NULL,
    `PRIMARY_DS_TYPE` VARCHAR(100) NOT NULL,
    CONSTRAINT `PK_PDT` PRIMARY KEY (`PRIMARY_DS_TYPE_ID`),
    CONSTRAINT `TUC_PDT_PRIMARY_DS_TYPE` UNIQUE (`PRIMARY_DS_TYPE`)
)
ENGINE = InnoDB ;

# ---------------------------------------------------------------------- #
# Add table "DATASET_ACCESS_TYPES"                                       #
# ---------------------------------------------------------------------- #

CREATE TABLE `DATASET_ACCESS_TYPES` (
    `DATASET_ACCESS_TYPE_ID` INTEGER NOT NULL,
    `DATASET_ACCESS_TYPE` VARCHAR(100) NOT NULL,
    CONSTRAINT `PK_DTP` PRIMARY KEY (`DATASET_ACCESS_TYPE_ID`),
    CONSTRAINT `TUC_DTP_DATASET_ACCESS_TYPE` UNIQUE (`DATASET_ACCESS_TYPE`)
)
ENGINE = InnoDB ;

# ---------------------------------------------------------------------- #
# Add table "PARAMETER_SET_HASHES"                                       #
# ---------------------------------------------------------------------- #

CREATE TABLE `PARAMETER_SET_HASHES` (
    `PARAMETER_SET_HASH_ID` INTEGER NOT NULL,
    `PSET_HASH` VARCHAR(128) NOT NULL,
    `NAME` VARCHAR(100),
    CONSTRAINT `PK_PSH` PRIMARY KEY (`PARAMETER_SET_HASH_ID`),
    CONSTRAINT `TUC_PSH_PSET_HASH` UNIQUE (`PSET_HASH`)
)
ENGINE = InnoDB ;

CREATE INDEX `IDX_PSH_1` ON `PARAMETER_SET_HASHES` (`NAME`);

# ---------------------------------------------------------------------- #
# Add table "DBS_VERSIONS"                                               #
# ---------------------------------------------------------------------- #

CREATE TABLE `DBS_VERSIONS` (
    `DBS_VERSION_ID` INTEGER NOT NULL,
    `SCHEMA_VERSION` VARCHAR(40) NOT NULL,
    `DBS_RELEASE_VERSION` VARCHAR(40) NOT NULL,
    `INSTANCE_NAME` VARCHAR(40) NOT NULL,
    `INSTANCE_TYPE` VARCHAR(40) NOT NULL,
    `CREATION_DATE` INTEGER,
    `LAST_MODIFICATION_DATE` INTEGER,
    CONSTRAINT `PK_DV` PRIMARY KEY (`DBS_VERSION_ID`)
)
ENGINE = InnoDB ;

# ---------------------------------------------------------------------- #
# Add table "SITES"                                                      #
# ---------------------------------------------------------------------- #

CREATE TABLE `SITES` (
    `SITE_ID` INTEGER NOT NULL,
    `SITE_NAME` VARCHAR(100) NOT NULL,
    CONSTRAINT `PK_SI` PRIMARY KEY (`SITE_ID`),
    CONSTRAINT `TUC_SI_SITE_NAME` UNIQUE (`SITE_NAME`)
)
ENGINE = InnoDB ;

# ---------------------------------------------------------------------- #
# Add table "OUTPUT_MODULE_CONFIGS"                                      #
# ---------------------------------------------------------------------- #

CREATE TABLE `OUTPUT_MODULE_CONFIGS` (
    `OUTPUT_MOD_CONFIG_ID` INTEGER NOT NULL,
    `APP_EXEC_ID` INTEGER NOT NULL,
    `RELEASE_VERSION_ID` INTEGER NOT NULL,
    `PARAMETER_SET_HASH_ID` INTEGER NOT NULL,
    `OUTPUT_MODULE_LABEL` VARCHAR(45) NOT NULL DEFAULT 'NONE',
    `CREATION_DATE` INTEGER,
    `CREATE_BY` VARCHAR(100),
    CONSTRAINT `PK_OMC` PRIMARY KEY (`OUTPUT_MOD_CONFIG_ID`),
    CONSTRAINT `TUC_OMC_1` UNIQUE (`APP_EXEC_ID`, `RELEASE_VERSION_ID`, `PARAMETER_SET_HASH_ID`, `OUTPUT_MODULE_LABEL`)
)
ENGINE = InnoDB ;

CREATE INDEX `ID_OMC_1` ON `OUTPUT_MODULE_CONFIGS` (`RELEASE_VERSION_ID`);

CREATE INDEX `ID_OMC_2` ON `OUTPUT_MODULE_CONFIGS` (`PARAMETER_SET_HASH_ID`);

CREATE INDEX `ID_OMC_3` ON `OUTPUT_MODULE_CONFIGS` (`OUTPUT_MODULE_LABEL`);

CREATE INDEX `ID_OMC_4` ON `OUTPUT_MODULE_CONFIGS` (`APP_EXEC_ID`);

# ---------------------------------------------------------------------- #
# Add table "DATA_TIERS"                                                 #
# ---------------------------------------------------------------------- #

CREATE TABLE `DATA_TIERS` (
    `DATA_TIER_ID` INTEGER NOT NULL,
    `DATA_TIER_NAME` VARCHAR(100) NOT NULL,
    `CREATION_DATE` INTEGER,
    `CREATE_BY` VARCHAR(100),
    CONSTRAINT `PK_DT` PRIMARY KEY (`DATA_TIER_ID`),
    CONSTRAINT `TUC_DT_DATA_TIER_NAME` UNIQUE (`DATA_TIER_NAME`)
)
ENGINE = InnoDB ;

# ---------------------------------------------------------------------- #
# Add table "PRIMARY_DATASETS"                                           #
# ---------------------------------------------------------------------- #

CREATE TABLE `PRIMARY_DATASETS` (
    `PRIMARY_DS_ID` INTEGER NOT NULL,
    `PRIMARY_DS_NAME` VARCHAR(100) NOT NULL,
    `PRIMARY_DS_TYPE_ID` INTEGER NOT NULL,
    `CREATION_DATE` INTEGER,
    `CREATE_BY` VARCHAR(100),
    CONSTRAINT `PK_PDS` PRIMARY KEY (`PRIMARY_DS_ID`),
    CONSTRAINT `TUC_PDS_PRIMARY_DS_NAME` UNIQUE (`PRIMARY_DS_NAME`)
)
ENGINE = InnoDB ;

CREATE INDEX `IDX_PDS_1` ON `PRIMARY_DATASETS` (`PRIMARY_DS_TYPE_ID`);

# ---------------------------------------------------------------------- #
# Add table "DATASETS"                                                   #
# ---------------------------------------------------------------------- #

CREATE TABLE `DATASETS` (
    `DATASET_ID` INTEGER NOT NULL,
    `DATASET` VARCHAR(700) NOT NULL,
    `IS_DATASET_VALID` INTEGER NOT NULL DEFAULT 1,
    `PRIMARY_DS_ID` INTEGER NOT NULL,
    `PROCESSED_DS_ID` INTEGER NOT NULL,
    `DATA_TIER_ID` INTEGER NOT NULL,
    `DATASET_ACCESS_TYPE_ID` INTEGER NOT NULL,
    `ACQUISITION_ERA_ID` INTEGER,
    `PROCESSING_ERA_ID` INTEGER,
    `PHYSICS_GROUP_ID` INTEGER,
    `XTCROSSSECTION` FLOAT(126,0),
    `GLOBAL_TAG` VARCHAR(255),
    `DATASET_UID` VARCHAR(100),
    `CREATION_DATE` INTEGER,
    `CREATE_BY` VARCHAR(100),
    `LAST_MODIFICATION_DATE` INTEGER,
    `LAST_MODIFIED_BY` VARCHAR(100),
    CONSTRAINT `PK_DS` PRIMARY KEY (`DATASET_ID`),
    CONSTRAINT `TUC_DS_DATASET` UNIQUE (`DATASET`)
)
ENGINE = InnoDB ;

CREATE INDEX `IDX_DS_1` ON `DATASETS` (`PRIMARY_DS_ID`);

CREATE INDEX `IDX_DS_2` ON `DATASETS` (`DATA_TIER_ID`);

CREATE INDEX `IDX_DS_3` ON `DATASETS` (`PROCESSED_DS_ID`);

CREATE INDEX `IDX_DS_4` ON `DATASETS` (`DATASET_ACCESS_TYPE_ID`);

CREATE INDEX `IDX_DS_5` ON `DATASETS` (`PHYSICS_GROUP_ID`);

CREATE INDEX `IDX_DS_6` ON `DATASETS` (`ACQUISITION_ERA_ID`);

CREATE INDEX `IDX_DS_7` ON `DATASETS` (`PROCESSING_ERA_ID`);

CREATE INDEX `ID_DS_8` ON `DATASETS` (`CREATION_DATE`);

CREATE INDEX `ID_DS_9` ON `DATASETS` (`CREATE_BY`);

ALTER TABLE `DATASETS` ADD CONSTRAINT `CC_DS_IS_DATASET_VALID` 
    CHECK (IS_DATASET_VALID in (1,0));

# ---------------------------------------------------------------------- #
# Add table "BLOCKS"                                                     #
# ---------------------------------------------------------------------- #

CREATE TABLE `BLOCKS` (
    `BLOCK_ID` INTEGER NOT NULL,
    `BLOCK_NAME` VARCHAR(500) NOT NULL,
    `DATASET_ID` INTEGER NOT NULL,
    `OPEN_FOR_WRITING` INTEGER NOT NULL DEFAULT 1,
    `ORIGIN_SITE_NAME` VARCHAR(100) NOT NULL,
    `BLOCK_SIZE` INTEGER,
    `FILE_COUNT` INTEGER,
    `CREATION_DATE` INTEGER,
    `CREATE_BY` VARCHAR(100),
    `LAST_MODIFICATION_DATE` INTEGER,
    `LAST_MODIFIED_BY` VARCHAR(100),
    CONSTRAINT `PK_BK` PRIMARY KEY (`BLOCK_ID`),
    CONSTRAINT `TUC_BK_BLOCK_NAME` UNIQUE (`BLOCK_NAME`)
)
ENGINE = InnoDB ;

CREATE INDEX `IDX_BK_1` ON `BLOCKS` (`DATASET_ID`);

CREATE INDEX `ID_BK_2` ON `BLOCKS` (`BLOCK_SIZE`);

CREATE INDEX `ID_BK_3` ON `BLOCKS` (`FILE_COUNT`);

CREATE INDEX `ID_BK_4` ON `BLOCKS` (`CREATION_DATE`);

CREATE INDEX `ID_BK_5` ON `BLOCKS` (`CREATE_BY`);

ALTER TABLE `BLOCKS` ADD CONSTRAINT `CC_BK_OPEN_FOR_WRITING` 
    CHECK (OPEN_FOR_WRITING IN (1,0));

# ---------------------------------------------------------------------- #
# Add table "BLOCK_PARENTS"                                              #
# ---------------------------------------------------------------------- #

CREATE TABLE `BLOCK_PARENTS` (
    `BLOCK_PARENT_ID` INTEGER NOT NULL,
    `THIS_BLOCK_ID` INTEGER NOT NULL,
    `PARENT_BLOCK_ID` INTEGER NOT NULL,
    CONSTRAINT `PK_BP` PRIMARY KEY (`BLOCK_PARENT_ID`),
    CONSTRAINT `TUC_BP_1` UNIQUE (`THIS_BLOCK_ID`, `PARENT_BLOCK_ID`)
)
ENGINE = InnoDB ;

CREATE INDEX `IDX_BP_1` ON `BLOCK_PARENTS` (`THIS_BLOCK_ID`);

CREATE INDEX `IDX_BP_2` ON `BLOCK_PARENTS` (`PARENT_BLOCK_ID`);

# ---------------------------------------------------------------------- #
# Add table "FILES"                                                      #
# ---------------------------------------------------------------------- #

CREATE TABLE `FILES` (
    `FILE_ID` INTEGER NOT NULL,
    `LOGICAL_FILE_NAME` VARCHAR(500) NOT NULL,
    `IS_FILE_VALID` INTEGER NOT NULL DEFAULT 1,
    `DATASET_ID` INTEGER NOT NULL,
    `BLOCK_ID` INTEGER NOT NULL,
    `FILE_TYPE_ID` INTEGER NOT NULL,
    `CHECK_SUM` VARCHAR(100) NOT NULL,
    `EVENT_COUNT` INTEGER NOT NULL,
    `FILE_SIZE` INTEGER NOT NULL,
    `BRANCH_HASH_ID` INTEGER,
    `ADLER32` VARCHAR(100) DEFAULT 'NOTSET',
    `MD5` VARCHAR(100) DEFAULT 'NOTSET',
    `AUTO_CROSS_SECTION` FLOAT(126,0),
    `CREATION_DATE` INTEGER,
    `CREATE_BY` VARCHAR(100),
    `LAST_MODIFICATION_DATE` INTEGER,
    `LAST_MODIFIED_BY` VARCHAR(100),
    CONSTRAINT `PK_FL` PRIMARY KEY (`FILE_ID`),
    CONSTRAINT `TUC_FL_LOGICAL_FILE_NAME` UNIQUE (`LOGICAL_FILE_NAME`)
)
ENGINE = InnoDB ;

CREATE INDEX `IDX_FL_1` ON `FILES` (`DATASET_ID`);

CREATE INDEX `IDX_FL_2` ON `FILES` (`BLOCK_ID`);

CREATE INDEX `IDX_FL_3` ON `FILES` (`FILE_TYPE_ID`);

CREATE INDEX `IDX_FL_4` ON `FILES` (`BRANCH_HASH_ID`);

CREATE INDEX `IDX_FL_5` ON `FILES` (`FILE_SIZE`);

CREATE INDEX `IDX_FL_6` ON `FILES` (`CREATION_DATE`);

CREATE INDEX `IDX_FL_7` ON `FILES` (`CREATE_BY`);

ALTER TABLE `FILES` ADD CONSTRAINT `CC_FL_IS_FILE_VALID` 
    CHECK (IS_FILE_VALID in (1,0));

# ---------------------------------------------------------------------- #
# Add table "DATASET_OUTPUT_MOD_CONFIGS"                                 #
# ---------------------------------------------------------------------- #

CREATE TABLE `DATASET_OUTPUT_MOD_CONFIGS` (
    `DS_OUTPUT_MOD_CONF_ID` INTEGER NOT NULL,
    `DATASET_ID` INTEGER NOT NULL,
    `OUTPUT_MOD_CONFIG_ID` INTEGER NOT NULL,
    CONSTRAINT `PK_DC` PRIMARY KEY (`DS_OUTPUT_MOD_CONF_ID`),
    CONSTRAINT `TUC_DC_1` UNIQUE (`DATASET_ID`, `OUTPUT_MOD_CONFIG_ID`)
)
ENGINE = InnoDB ;

CREATE INDEX `IDX_DC_1` ON `DATASET_OUTPUT_MOD_CONFIGS` (`DATASET_ID`);

CREATE INDEX `IDX_DC_2` ON `DATASET_OUTPUT_MOD_CONFIGS` (`OUTPUT_MOD_CONFIG_ID`);

# ---------------------------------------------------------------------- #
# Add table "DATASET_PARENTS"                                            #
# ---------------------------------------------------------------------- #

CREATE TABLE `DATASET_PARENTS` (
    `DATASET_PARENT_ID` INTEGER NOT NULL,
    `THIS_DATASET_ID` INTEGER NOT NULL,
    `PARENT_DATASET_ID` INTEGER NOT NULL,
    CONSTRAINT `PK_DP` PRIMARY KEY (`DATASET_PARENT_ID`),
    CONSTRAINT `TUC_DP_1` UNIQUE (`THIS_DATASET_ID`, `PARENT_DATASET_ID`)
)
ENGINE = InnoDB ;

CREATE INDEX `IDX_DP_1` ON `DATASET_PARENTS` (`THIS_DATASET_ID`);

CREATE INDEX `IDX_DP_2` ON `DATASET_PARENTS` (`PARENT_DATASET_ID`);

# ---------------------------------------------------------------------- #
# Add table "DATASET_RUNS"                                               #
# ---------------------------------------------------------------------- #

CREATE TABLE `DATASET_RUNS` (
    `DATASET_RUN_ID` INTEGER NOT NULL,
    `DATASET_ID` INTEGER NOT NULL,
    `RUN_NUMBER` INTEGER,
    `COMPLETE` INTEGER DEFAULT 0,
    `LUMI_SECTION_COUNT` INTEGER,
    `CREATION_DATE` INTEGER,
    `CREATE_BY` VARCHAR(100),
    CONSTRAINT `PK_DR` PRIMARY KEY (`DATASET_RUN_ID`)
)
ENGINE = InnoDB ;

CREATE INDEX `IDX_DR_1` ON `DATASET_RUNS` (`DATASET_ID`);

CREATE INDEX `IDX_DR_2` ON `DATASET_RUNS` (`RUN_NUMBER`);

CREATE INDEX `IDX_DR_3` ON `DATASET_RUNS` (`LUMI_SECTION_COUNT`);

# ---------------------------------------------------------------------- #
# Add table "FILE_OUTPUT_MOD_CONFIGS"                                    #
# ---------------------------------------------------------------------- #

CREATE TABLE `FILE_OUTPUT_MOD_CONFIGS` (
    `FILE_OUTPUT_CONFIG_ID` INTEGER NOT NULL,
    `FILE_ID` INTEGER NOT NULL,
    `OUTPUT_MOD_CONFIG_ID` INTEGER NOT NULL,
    CONSTRAINT `PK_FC` PRIMARY KEY (`FILE_OUTPUT_CONFIG_ID`),
    CONSTRAINT `TUC_FC_1` UNIQUE (`FILE_ID`, `OUTPUT_MOD_CONFIG_ID`)
)
ENGINE = InnoDB ;

CREATE INDEX `IDX_FC_1` ON `FILE_OUTPUT_MOD_CONFIGS` (`FILE_ID`);

CREATE INDEX `IDX_FC_2` ON `FILE_OUTPUT_MOD_CONFIGS` (`OUTPUT_MOD_CONFIG_ID`);

# ---------------------------------------------------------------------- #
# Add table "ASSOCIATED_FILES"                                           #
# ---------------------------------------------------------------------- #

CREATE TABLE `ASSOCIATED_FILES` (
    `ASSOCATED_FILE_ID` INTEGER NOT NULL,
    `THIS_FILE_ID` INTEGER NOT NULL,
    `ASSOCATED_FILE` INTEGER NOT NULL,
    CONSTRAINT `PK_AF` PRIMARY KEY (`ASSOCATED_FILE_ID`),
    CONSTRAINT `TUC_AF_1` UNIQUE (`THIS_FILE_ID`, `ASSOCATED_FILE`)
)
ENGINE = InnoDB ;

CREATE INDEX `IDX_AF_1` ON `ASSOCIATED_FILES` (`THIS_FILE_ID`);

CREATE INDEX `IDX_AF_2` ON `ASSOCIATED_FILES` (`ASSOCATED_FILE`);

# ---------------------------------------------------------------------- #
# Add table "FILE_PARENTS"                                               #
# ---------------------------------------------------------------------- #

CREATE TABLE `FILE_PARENTS` (
    `FILE_PARENT_ID` INTEGER NOT NULL,
    `THIS_FILE_ID` INTEGER NOT NULL,
    `PARENT_FILE_ID` INTEGER NOT NULL,
    CONSTRAINT `PK_FP` PRIMARY KEY (`FILE_PARENT_ID`),
    CONSTRAINT `TUC_FP_1` UNIQUE (`THIS_FILE_ID`, `PARENT_FILE_ID`)
)
ENGINE = InnoDB ;

CREATE INDEX `IDX_FP_1` ON `FILE_PARENTS` (`THIS_FILE_ID`);

CREATE INDEX `IDX_FP_2` ON `FILE_PARENTS` (`PARENT_FILE_ID`);

# ---------------------------------------------------------------------- #
# Add table "FILE_LUMIS"                                                 #
# ---------------------------------------------------------------------- #

CREATE TABLE `FILE_LUMIS` (
    `FILE_LUMI_ID` INTEGER NOT NULL,
    `RUN_NUM` INTEGER NOT NULL,
    `LUMI_SECTION_NUM` INTEGER NOT NULL,
    `FILE_ID` INTEGER NOT NULL,
    CONSTRAINT `PK_FLM` PRIMARY KEY (`FILE_LUMI_ID`),
    CONSTRAINT `TUC_FLM_1` UNIQUE (`RUN_NUM`, `LUMI_SECTION_NUM`, `FILE_ID`)
)
ENGINE = InnoDB ;

CREATE INDEX `IDX_FLM_1` ON `FILE_LUMIS` (`FILE_ID`);

# ---------------------------------------------------------------------- #
# Add table "ACQUISITION_ERAS"                                           #
# ---------------------------------------------------------------------- #

CREATE TABLE `ACQUISITION_ERAS` (
    `ACQUISITION_ERA_ID` INTEGER NOT NULL,
    `ACQUISITION_ERA_NAME` VARCHAR(120) NOT NULL,
    `CREATION_DATE` INTEGER,
    `CREATE_BY` VARCHAR(100),
    `DESCRIPTION` VARCHAR(40),
    CONSTRAINT `PK_AQE` PRIMARY KEY (`ACQUISITION_ERA_ID`),
    CONSTRAINT `TUC_AQE_ACQUISITION_ERA_NAME` UNIQUE (`ACQUISITION_ERA_NAME`)
)
ENGINE = InnoDB ;

# ---------------------------------------------------------------------- #
# Add table "PROCESSING_ERAS"                                            #
# ---------------------------------------------------------------------- #

CREATE TABLE `PROCESSING_ERAS` (
    `PROCESSING_ERA_ID` INTEGER NOT NULL,
    `PROCESSING_VERSION` VARCHAR(4),
    `CREATION_DATE` INTEGER,
    `CREATE_BY` VARCHAR(100),
    `DESCRIPTION` VARCHAR(40),
    CONSTRAINT `PK_PE` PRIMARY KEY (`PROCESSING_ERA_ID`),
    CONSTRAINT `TUC_PE_PROCESSING_VERSION` UNIQUE (`PROCESSING_VERSION`)
)
ENGINE = InnoDB ;

# ---------------------------------------------------------------------- #
# Add table "BLOCK_SITES"                                                #
# ---------------------------------------------------------------------- #

CREATE TABLE `BLOCK_SITES` (
    `BLOCK_SITE_ID` INTEGER NOT NULL,
    `BLOCK_ID` INTEGER NOT NULL,
    `SITE_ID` INTEGER NOT NULL,
    CONSTRAINT `PK_BLST` PRIMARY KEY (`BLOCK_SITE_ID`)
)
ENGINE = InnoDB ;

# ---------------------------------------------------------------------- #
# Add table "FILE_BUFFERS"                                               #
# ---------------------------------------------------------------------- #

CREATE TABLE `FILE_BUFFERS` (
    `LOGICAL_FILE_NAME` VARCHAR(700) NOT NULL,
    `FILE_CLOB` LONGTEXT,
    `BLOCK_ID` INTEGER,
    CONSTRAINT `PK_FB` PRIMARY KEY (`LOGICAL_FILE_NAME`)
)
ENGINE = InnoDB ;

# ---------------------------------------------------------------------- #
# Add table "MIGRATION_REQUESTS"                                         #
# ---------------------------------------------------------------------- #

CREATE TABLE `MIGRATION_REQUESTS` (
    `MIGRATION_REQUEST_ID` INTEGER NOT NULL,
    `MIGRATION_URL` VARCHAR(300),
    `MIGRATION_INPUT` VARCHAR(700),
    `MIGRATION_STATUS` INTEGER,
    `CREATION_DATE` INTEGER,
    `CREATE_BY` VARCHAR(100),
    `LAST_MODIFICATION_DATE` INTEGER,
    `LAST_MODIFIED_BY` VARCHAR(100),
    CONSTRAINT `PK_MR` PRIMARY KEY (`MIGRATION_REQUEST_ID`),
    CONSTRAINT `TUC_MR_1` UNIQUE (`MIGRATION_URL`, `MIGRATION_INPUT`)
)
ENGINE = InnoDB ;

# ---------------------------------------------------------------------- #
# Add table "MIGRATION_BLOCKS"                                           #
# ---------------------------------------------------------------------- #

CREATE TABLE `MIGRATION_BLOCKS` (
    `MIGRATION_BLOCK_ID` INTEGER NOT NULL,
    `MIGRATION_REQUEST_ID` INTEGER NOT NULL,
    `MIGRATION_BLOCK_NAME` VARCHAR(700),
    `MIGRATION_ORDER` INTEGER,
    `MIGRATION_STATUS` INTEGER,
    `CREATION_DATE` INTEGER,
    `CREATE_BY` VARCHAR(100),
    `LAST_MODIFICATION_DATE` INTEGER,
    `LAST_MODIFIED_BY` VARCHAR(100),
    CONSTRAINT `PK_MB` PRIMARY KEY (`MIGRATION_BLOCK_ID`),
    CONSTRAINT `TUC_MB_1` UNIQUE (`MIGRATION_BLOCK_NAME`, `MIGRATION_REQUEST_ID`)
)
ENGINE = InnoDB ;

# ---------------------------------------------------------------------- #
# Add table "COMPONENT_STATUS"                                           #
# ---------------------------------------------------------------------- #

CREATE TABLE `COMPONENT_STATUS` (
    `COMP_STATUS_ID` INTEGER NOT NULL,
    `COMPONENT_NAME` VARCHAR(50),
    `COMPONENT_STATUS` VARCHAR(100),
    `LAST_CONTACT_TIME` INTEGER,
    CONSTRAINT `PK_CSS` PRIMARY KEY (`COMP_STATUS_ID`)
)
ENGINE = InnoDB ;

# ---------------------------------------------------------------------- #
# Foreign key constraints                                                #
# ---------------------------------------------------------------------- #

ALTER TABLE `OUTPUT_MODULE_CONFIGS` ADD CONSTRAINT `AE_OMC` 
    FOREIGN KEY (`APP_EXEC_ID`) REFERENCES `APPLICATION_EXECUTABLES` (`APP_EXEC_ID`) ON DELETE CASCADE;

ALTER TABLE `OUTPUT_MODULE_CONFIGS` ADD CONSTRAINT `RV_OMC` 
    FOREIGN KEY (`RELEASE_VERSION_ID`) REFERENCES `RELEASE_VERSIONS` (`RELEASE_VERSION_ID`) ON DELETE CASCADE;

ALTER TABLE `OUTPUT_MODULE_CONFIGS` ADD CONSTRAINT `PSH_OMC` 
    FOREIGN KEY (`PARAMETER_SET_HASH_ID`) REFERENCES `PARAMETER_SET_HASHES` (`PARAMETER_SET_HASH_ID`) ON DELETE CASCADE;

ALTER TABLE `PRIMARY_DATASETS` ADD CONSTRAINT `PDT_PDS` 
    FOREIGN KEY (`PRIMARY_DS_TYPE_ID`) REFERENCES `PRIMARY_DS_TYPES` (`PRIMARY_DS_TYPE_ID`);

ALTER TABLE `DATASETS` ADD CONSTRAINT `PDS_DS` 
    FOREIGN KEY (`PRIMARY_DS_ID`) REFERENCES `PRIMARY_DATASETS` (`PRIMARY_DS_ID`) ON DELETE CASCADE;

ALTER TABLE `DATASETS` ADD CONSTRAINT `DT_DS` 
    FOREIGN KEY (`DATA_TIER_ID`) REFERENCES `DATA_TIERS` (`DATA_TIER_ID`) ON DELETE CASCADE;

ALTER TABLE `DATASETS` ADD CONSTRAINT `PSDS_DS` 
    FOREIGN KEY (`PROCESSED_DS_ID`) REFERENCES `PROCESSED_DATASETS` (`PROCESSED_DS_ID`) ON DELETE CASCADE;

ALTER TABLE `DATASETS` ADD CONSTRAINT `DTP_DS` 
    FOREIGN KEY (`DATASET_ACCESS_TYPE_ID`) REFERENCES `DATASET_ACCESS_TYPES` (`DATASET_ACCESS_TYPE_ID`);

ALTER TABLE `DATASETS` ADD CONSTRAINT `PG_DS` 
    FOREIGN KEY (`PHYSICS_GROUP_ID`) REFERENCES `PHYSICS_GROUPS` (`PHYSICS_GROUP_ID`) ON DELETE SET NULL;

ALTER TABLE `DATASETS` ADD CONSTRAINT `AQE_DS` 
    FOREIGN KEY (`ACQUISITION_ERA_ID`) REFERENCES `ACQUISITION_ERAS` (`ACQUISITION_ERA_ID`) ON DELETE SET NULL;

ALTER TABLE `DATASETS` ADD CONSTRAINT `PE_DS` 
    FOREIGN KEY (`PROCESSING_ERA_ID`) REFERENCES `PROCESSING_ERAS` (`PROCESSING_ERA_ID`) ON DELETE SET NULL;

ALTER TABLE `BLOCKS` ADD CONSTRAINT `DS_BK` 
    FOREIGN KEY (`DATASET_ID`) REFERENCES `DATASETS` (`DATASET_ID`) ON DELETE CASCADE;

ALTER TABLE `BLOCK_PARENTS` ADD CONSTRAINT `BK_BP` 
    FOREIGN KEY (`THIS_BLOCK_ID`) REFERENCES `BLOCKS` (`BLOCK_ID`) ON DELETE CASCADE;

ALTER TABLE `BLOCK_PARENTS` ADD CONSTRAINT `BK_BP2` 
    FOREIGN KEY (`PARENT_BLOCK_ID`) REFERENCES `BLOCKS` (`BLOCK_ID`) ON DELETE CASCADE;

ALTER TABLE `FILES` ADD CONSTRAINT `DS_FL` 
    FOREIGN KEY (`DATASET_ID`) REFERENCES `DATASETS` (`DATASET_ID`) ON DELETE CASCADE;

ALTER TABLE `FILES` ADD CONSTRAINT `BK_FL` 
    FOREIGN KEY (`BLOCK_ID`) REFERENCES `BLOCKS` (`BLOCK_ID`) ON DELETE CASCADE;

ALTER TABLE `FILES` ADD CONSTRAINT `FT_FL` 
    FOREIGN KEY (`FILE_TYPE_ID`) REFERENCES `FILE_DATA_TYPES` (`FILE_TYPE_ID`);

ALTER TABLE `FILES` ADD CONSTRAINT `BH_FL` 
    FOREIGN KEY (`BRANCH_HASH_ID`) REFERENCES `BRANCH_HASHES` (`BRANCH_HASH_ID`) ON DELETE SET NULL;

ALTER TABLE `DATASET_OUTPUT_MOD_CONFIGS` ADD CONSTRAINT `DS_DC` 
    FOREIGN KEY (`DATASET_ID`) REFERENCES `DATASETS` (`DATASET_ID`) ON DELETE CASCADE;

ALTER TABLE `DATASET_OUTPUT_MOD_CONFIGS` ADD CONSTRAINT `OMC_DC` 
    FOREIGN KEY (`OUTPUT_MOD_CONFIG_ID`) REFERENCES `OUTPUT_MODULE_CONFIGS` (`OUTPUT_MOD_CONFIG_ID`) ON DELETE CASCADE;

ALTER TABLE `DATASET_PARENTS` ADD CONSTRAINT `DS_DP` 
    FOREIGN KEY (`THIS_DATASET_ID`) REFERENCES `DATASETS` (`DATASET_ID`) ON DELETE CASCADE;

ALTER TABLE `DATASET_PARENTS` ADD CONSTRAINT `DS_DP2` 
    FOREIGN KEY (`PARENT_DATASET_ID`) REFERENCES `DATASETS` (`DATASET_ID`) ON DELETE CASCADE;

ALTER TABLE `DATASET_RUNS` ADD CONSTRAINT `DS_DR` 
    FOREIGN KEY (`DATASET_ID`) REFERENCES `DATASETS` (`DATASET_ID`) ON DELETE CASCADE;

ALTER TABLE `FILE_OUTPUT_MOD_CONFIGS` ADD CONSTRAINT `FL_FC` 
    FOREIGN KEY (`FILE_ID`) REFERENCES `FILES` (`FILE_ID`) ON DELETE CASCADE;

ALTER TABLE `FILE_OUTPUT_MOD_CONFIGS` ADD CONSTRAINT `OMC_FC` 
    FOREIGN KEY (`OUTPUT_MOD_CONFIG_ID`) REFERENCES `OUTPUT_MODULE_CONFIGS` (`OUTPUT_MOD_CONFIG_ID`) ON DELETE CASCADE;

ALTER TABLE `ASSOCIATED_FILES` ADD CONSTRAINT `FL_AF` 
    FOREIGN KEY (`THIS_FILE_ID`) REFERENCES `FILES` (`FILE_ID`) ON DELETE CASCADE;

ALTER TABLE `ASSOCIATED_FILES` ADD CONSTRAINT `FL_AF2` 
    FOREIGN KEY (`ASSOCATED_FILE`) REFERENCES `FILES` (`FILE_ID`) ON DELETE CASCADE;

ALTER TABLE `FILE_PARENTS` ADD CONSTRAINT `FL_FP` 
    FOREIGN KEY (`THIS_FILE_ID`) REFERENCES `FILES` (`FILE_ID`) ON DELETE CASCADE;

ALTER TABLE `FILE_PARENTS` ADD CONSTRAINT `FL_FP2` 
    FOREIGN KEY (`PARENT_FILE_ID`) REFERENCES `FILES` (`FILE_ID`) ON DELETE CASCADE;

ALTER TABLE `FILE_LUMIS` ADD CONSTRAINT `FL_FLM` 
    FOREIGN KEY (`FILE_ID`) REFERENCES `FILES` (`FILE_ID`) ON DELETE CASCADE;

ALTER TABLE `BLOCK_SITES` ADD CONSTRAINT `BK_BLST` 
    FOREIGN KEY (`BLOCK_ID`) REFERENCES `BLOCKS` (`BLOCK_ID`);

ALTER TABLE `BLOCK_SITES` ADD CONSTRAINT `SI_BLST` 
    FOREIGN KEY (`SITE_ID`) REFERENCES `SITES` (`SITE_ID`);

ALTER TABLE `MIGRATION_BLOCKS` ADD CONSTRAINT `MR_MB` 
    FOREIGN KEY (`MIGRATION_REQUEST_ID`) REFERENCES `MIGRATION_REQUESTS` (`MIGRATION_REQUEST_ID`) ON DELETE CASCADE;
