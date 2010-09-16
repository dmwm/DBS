# ---------------------------------------------------------------------- #
# Script generated with: DeZign for Databases v5.2.3                     #
# Target DBMS:           MySQL 5                                         #
# Project file:          Mysql_Seq.dez                                   #
# Project name:                                                          #
# Author:                                                                #
# Script type:           Database creation script                        #
# Created on:            2010-02-11 14:21                                #
# Model version:         Version 2010-02-11                              #
# ---------------------------------------------------------------------- #


# ---------------------------------------------------------------------- #
# Tables                                                                 #
# ---------------------------------------------------------------------- #

# ---------------------------------------------------------------------- #
# Add table "SEQ_RVS"                                                    #
# ---------------------------------------------------------------------- #

CREATE TABLE `SEQ_RVS` (
    `RELEASE_VERSION_ID` BIGINT NOT NULL,
    CONSTRAINT `PK_SEQ_RVS` PRIMARY KEY (`RELEASE_VERSION_ID`)
);

# ---------------------------------------------------------------------- #
# Add table "SEQ_PSHS"                                                   #
# ---------------------------------------------------------------------- #

CREATE TABLE `SEQ_PSHS` (
    `PARAMETER_SET_HASHE_ID` BIGINT NOT NULL,
    CONSTRAINT `PK_SEQ_PSHS` PRIMARY KEY (`PARAMETER_SET_HASHE_ID`)
);

# ---------------------------------------------------------------------- #
# Add table "SEQ_DRS"                                                    #
# ---------------------------------------------------------------------- #

CREATE TABLE `SEQ_DRS` (
    `PATH_RUN_ID` BIGINT NOT NULL,
    CONSTRAINT `PK_SEQ_DRS` PRIMARY KEY (`PATH_RUN_ID`)
);

# ---------------------------------------------------------------------- #
# Add table "SEQ_PGS"                                                    #
# ---------------------------------------------------------------------- #

CREATE TABLE `SEQ_PGS` (
    `PHYSICS_GROUP_ID` BIGINT NOT NULL,
    CONSTRAINT `PK_SEQ_PGS` PRIMARY KEY (`PHYSICS_GROUP_ID`)
);

# ---------------------------------------------------------------------- #
# Add table "SEQ_DTS"                                                    #
# ---------------------------------------------------------------------- #

CREATE TABLE `SEQ_DTS` (
    `DATA_TIER_ID` BIGINT NOT NULL,
    CONSTRAINT `PK_SEQ_DTS` PRIMARY KEY (`DATA_TIER_ID`)
);

# ---------------------------------------------------------------------- #
# Add table "SEQ_PDSS"                                                   #
# ---------------------------------------------------------------------- #

CREATE TABLE `SEQ_PDSS` (
    `PRIMARY_DS_ID` BIGINT NOT NULL,
    CONSTRAINT `PK_SEQ_PDSS` PRIMARY KEY (`PRIMARY_DS_ID`)
);

# ---------------------------------------------------------------------- #
# Add table "SEQ_PDTS"                                                   #
# ---------------------------------------------------------------------- #

CREATE TABLE `SEQ_PDTS` (
    `PRIMARY_DS_TYPE_ID` BIGINT NOT NULL,
    CONSTRAINT `PK_SEQ_PDTS` PRIMARY KEY (`PRIMARY_DS_TYPE_ID`)
);

# ---------------------------------------------------------------------- #
# Add table "SEQ_OMCS"                                                   #
# ---------------------------------------------------------------------- #

CREATE TABLE `SEQ_OMCS` (
    `OUTPUT_MOD_CONFIG_ID` BIGINT NOT NULL,
    CONSTRAINT `PK_SEQ_OMCS` PRIMARY KEY (`OUTPUT_MOD_CONFIG_ID`)
);

# ---------------------------------------------------------------------- #
# Add table "SEQ_DCS"                                                    #
# ---------------------------------------------------------------------- #

CREATE TABLE `SEQ_DCS` (
    `DS_OUTPUT_MOD_CONF_ID` BIGINT NOT NULL,
    CONSTRAINT `PK_SEQ_DCS` PRIMARY KEY (`DS_OUTPUT_MOD_CONF_ID`)
);

# ---------------------------------------------------------------------- #
# Add table "SEQ_DPS"                                                    #
# ---------------------------------------------------------------------- #

CREATE TABLE `SEQ_DPS` (
    `DATASET_PARENT_ID` BIGINT NOT NULL,
    CONSTRAINT `PK_SEQ_DPS` PRIMARY KEY (`DATASET_PARENT_ID`)
);

# ---------------------------------------------------------------------- #
# Add table "SEQ_DSS"                                                    #
# ---------------------------------------------------------------------- #

CREATE TABLE `SEQ_DSS` (
    `DATASET_ID` BIGINT NOT NULL,
    CONSTRAINT `PK_SEQ_DSS` PRIMARY KEY (`DATASET_ID`)
);

# ---------------------------------------------------------------------- #
# Add table "SEQ_PSDS"                                                   #
# ---------------------------------------------------------------------- #

CREATE TABLE `SEQ_PSDS` (
    `PROCESSED_DS_ID` BIGINT NOT NULL,
    CONSTRAINT `PK_SEQ_PSDS` PRIMARY KEY (`PROCESSED_DS_ID`)
);

# ---------------------------------------------------------------------- #
# Add table "SEQ_AQES"                                                   #
# ---------------------------------------------------------------------- #

CREATE TABLE `SEQ_AQES` (
    `ACQUISITION_ERA_ID` BIGINT NOT NULL,
    CONSTRAINT `PK_SEQ_AQES` PRIMARY KEY (`ACQUISITION_ERA_ID`)
);

# ---------------------------------------------------------------------- #
# Add table "SEQ_AES"                                                    #
# ---------------------------------------------------------------------- #

CREATE TABLE `SEQ_AES` (
    `APP_EXEC_ID` BIGINT NOT NULL,
    CONSTRAINT `PK_SEQ_AES` PRIMARY KEY (`APP_EXEC_ID`)
);

# ---------------------------------------------------------------------- #
# Add table "SEQ_BPS"                                                    #
# ---------------------------------------------------------------------- #

CREATE TABLE `SEQ_BPS` (
    `BLOCK_PARENT_ID` BIGINT NOT NULL,
    CONSTRAINT `PK_SEQ_BPS` PRIMARY KEY (`BLOCK_PARENT_ID`)
);

# ---------------------------------------------------------------------- #
# Add table "SEQ_DTPS"                                                   #
# ---------------------------------------------------------------------- #

CREATE TABLE `SEQ_DTPS` (
    `DATASET_TYPE_ID` BIGINT NOT NULL,
    CONSTRAINT `PK_SEQ_DTPS` PRIMARY KEY (`DATASET_TYPE_ID`)
);

# ---------------------------------------------------------------------- #
# Add table "SEQ_PES"                                                    #
# ---------------------------------------------------------------------- #

CREATE TABLE `SEQ_PES` (
    `PROCESSING_ERA_ID` BIGINT NOT NULL,
    CONSTRAINT `PK_SEQ_PES` PRIMARY KEY (`PROCESSING_ERA_ID`)
);

# ---------------------------------------------------------------------- #
# Add table "SEQ_FLMS"                                                   #
# ---------------------------------------------------------------------- #

CREATE TABLE `SEQ_FLMS` (
    `FILE_LUMI_ID` BIGINT NOT NULL,
    CONSTRAINT `PK_SEQ_FLMS` PRIMARY KEY (`FILE_LUMI_ID`)
);

# ---------------------------------------------------------------------- #
# Add table "SEQ_SIS"                                                    #
# ---------------------------------------------------------------------- #

CREATE TABLE `SEQ_SIS` (
    `SITE_ID` BIGINT NOT NULL,
    CONSTRAINT `PK_SEQ_SIS` PRIMARY KEY (`SITE_ID`)
);

# ---------------------------------------------------------------------- #
# Add table "SEQ_BKS"                                                    #
# ---------------------------------------------------------------------- #

CREATE TABLE `SEQ_BKS` (
    `BLOCK_ID` BIGINT NOT NULL,
    CONSTRAINT `PK_SEQ_BKS` PRIMARY KEY (`BLOCK_ID`)
);

# ---------------------------------------------------------------------- #
# Add table "SEQ_FLS"                                                    #
# ---------------------------------------------------------------------- #

CREATE TABLE `SEQ_FLS` (
    `FILE_ID` BIGINT NOT NULL,
    CONSTRAINT `PK_SEQ_FLS` PRIMARY KEY (`FILE_ID`)
);

# ---------------------------------------------------------------------- #
# Add table "SEQ_FPS"                                                    #
# ---------------------------------------------------------------------- #

CREATE TABLE `SEQ_FPS` (
    `FILE_PARENT_ID` BIGINT NOT NULL,
    CONSTRAINT `PK_SEQ_FPS` PRIMARY KEY (`FILE_PARENT_ID`)
);

# ---------------------------------------------------------------------- #
# Add table "SEQ_FTS"                                                    #
# ---------------------------------------------------------------------- #

CREATE TABLE `SEQ_FTS` (
    `FILE_PARENT_ID` BIGINT NOT NULL,
    CONSTRAINT `PK_SEQ_FTS` PRIMARY KEY (`FILE_PARENT_ID`)
);

# ---------------------------------------------------------------------- #
# Add table "SEQ_SES"                                                    #
# ---------------------------------------------------------------------- #

CREATE TABLE `SEQ_SES` (
    `SE_ID` BIGINT NOT NULL,
    CONSTRAINT `PK_SEQ_SES` PRIMARY KEY (`SE_ID`)
);

# ---------------------------------------------------------------------- #
# Add table "SEQ_BSES"                                                   #
# ---------------------------------------------------------------------- #

CREATE TABLE `SEQ_BSES` (
    `BLOCK_SE_ID` BIGINT NOT NULL,
    CONSTRAINT `PK_SEQ_BSES` PRIMARY KEY (`BLOCK_SE_ID`)
);

# ---------------------------------------------------------------------- #
# Add table "SEQ_FCS"                                                    #
# ---------------------------------------------------------------------- #

CREATE TABLE `SEQ_FCS` (
    `FILE_OUTPUT_CONFIG_ID` BIGINT NOT NULL,
    CONSTRAINT `PK_SEQ_FCS` PRIMARY KEY (`FILE_OUTPUT_CONFIG_ID`)
);

# ---------------------------------------------------------------------- #
# Add table "SEQ_AFS"                                                    #
# ---------------------------------------------------------------------- #

CREATE TABLE `SEQ_AFS` (
    `ASSOCATED_FILE_ID` BIGINT NOT NULL,
    CONSTRAINT `PK_SEQ_AFS` PRIMARY KEY (`ASSOCATED_FILE_ID`)
);

# ---------------------------------------------------------------------- #
# Add table "SEQ_BHS"                                                    #
# ---------------------------------------------------------------------- #

CREATE TABLE `SEQ_BHS` (
    `BRANCH_HASH_ID` BIGINT NOT NULL,
    CONSTRAINT `PK_SEQ_BHS` PRIMARY KEY (`BRANCH_HASH_ID`)
);

# ---------------------------------------------------------------------- #
# Add table "SEQ_DVS"                                                    #
# ---------------------------------------------------------------------- #

CREATE TABLE `SEQ_DVS` (
    `SCHEMA_VERSION_ID` BIGINT NOT NULL,
    CONSTRAINT `PK_SEQ_DVS` PRIMARY KEY (`SCHEMA_VERSION_ID`)
);
