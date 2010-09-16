
CREATE OR REPLACE TRIGGER AE_TRIG before insert on APPLICATION_EXECUTABLES for each row begin if :NEW.APP_EXEC_ID is null then select SEQ_AE.nextval into :NEW.APP_EXEC_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER RV_TRIG before insert on RELEASE_VERSIONS for each row begin if :NEW.RELEASE_VERSION_ID is null then select SEQ_RV.nextval into :NEW.RELEASE_VERSION_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER PSDS_TRIG before insert on PROCESSED_DATASETS for each row begin if :NEW.PROCESSED_DS_ID is null then select SEQ_PSDS.nextval into :NEW.PROCESSED_DS_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER BH_TRIG before insert on BRANCH_HASHES for each row begin if :NEW.BRANCH_HASH_ID is null then select SEQ_BH.nextval into :NEW.BRANCH_HASH_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER FT_TRIG before insert on FILE_DATA_TYPES for each row begin if :NEW.FILE_TYPE_ID is null then select SEQ_FT.nextval into :NEW.FILE_TYPE_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER PG_TRIG before insert on PHYSICS_GROUPS for each row begin if :NEW.PHYSICS_GROUP_ID is null then select SEQ_PG.nextval into :NEW.PHYSICS_GROUP_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER PDT_TRIG before insert on PRIMARY_DS_TYPES for each row begin if :NEW.PRIMARY_DS_TYPE_ID is null then select SEQ_PDT.nextval into :NEW.PRIMARY_DS_TYPE_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER DTP_TRIG before insert on DATASET_ACCESS_TYPES for each row begin if :NEW.DATASET_ACCESS_TYPE_ID is null then select SEQ_DTP.nextval into :NEW.DATASET_ACCESS_TYPE_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER PSH_TRIG before insert on PARAMETER_SET_HASHES for each row begin if :NEW.PARAMETER_SET_HASH_ID is null then select SEQ_PSH.nextval into :NEW.PARAMETER_SET_HASH_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER DV_TRIG before insert on DBS_VERSIONS for each row begin if :NEW.DBS_VERSION_ID is null then select SEQ_DV.nextval into :NEW.DBS_VERSION_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER SI_TRIG before insert on SITES for each row begin if :NEW.SITE_ID is null then select SEQ_SI.nextval into :NEW.SITE_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER OMC_TRIG before insert on OUTPUT_MODULE_CONFIGS for each row begin if :NEW.OUTPUT_MOD_CONFIG_ID is null then select SEQ_OMC.nextval into :NEW.OUTPUT_MOD_CONFIG_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER DT_TRIG before insert on DATA_TIERS for each row begin if :NEW.DATA_TIER_ID is null then select SEQ_DT.nextval into :NEW.DATA_TIER_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER PDS_TRIG before insert on PRIMARY_DATASETS for each row begin if :NEW.PRIMARY_DS_ID is null then select SEQ_PDS.nextval into :NEW.PRIMARY_DS_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER DS_TRIG before insert on DATASETS for each row begin if :NEW.DATASET_ID is null then select SEQ_DS.nextval into :NEW.DATASET_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER BK_TRIG before insert on BLOCKS for each row begin if :NEW.BLOCK_ID is null then select SEQ_BK.nextval into :NEW.BLOCK_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER BP_TRIG before insert on BLOCK_PARENTS for each row begin if :NEW.BLOCK_PARENT_ID is null then select SEQ_BP.nextval into :NEW.BLOCK_PARENT_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER FL_TRIG before insert on FILES for each row begin if :NEW.FILE_ID is null then select SEQ_FL.nextval into :NEW.FILE_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER DC_TRIG before insert on DATASET_OUTPUT_MOD_CONFIGS for each row begin if :NEW.DS_OUTPUT_MOD_CONF_ID is null then select SEQ_DC.nextval into :NEW.DS_OUTPUT_MOD_CONF_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER DP_TRIG before insert on DATASET_PARENTS for each row begin if :NEW.DATASET_PARENT_ID is null then select SEQ_DP.nextval into :NEW.DATASET_PARENT_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER DR_TRIG before insert on DATASET_RUNS for each row begin if :NEW.DATASET_RUN_ID is null then select SEQ_DR.nextval into :NEW.DATASET_RUN_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER FC_TRIG before insert on FILE_OUTPUT_MOD_CONFIGS for each row begin if :NEW.FILE_OUTPUT_CONFIG_ID is null then select SEQ_FC.nextval into :NEW.FILE_OUTPUT_CONFIG_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER AF_TRIG before insert on ASSOCIATED_FILES for each row begin if :NEW.ASSOCATED_FILE_ID is null then select SEQ_AF.nextval into :NEW.ASSOCATED_FILE_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER FP_TRIG before insert on FILE_PARENTS for each row begin if :NEW.FILE_PARENT_ID is null then select SEQ_FP.nextval into :NEW.FILE_PARENT_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER FLM_TRIG before insert on FILE_LUMIS for each row begin if :NEW.FILE_LUMI_ID is null then select SEQ_FLM.nextval into :NEW.FILE_LUMI_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER AQE_TRIG before insert on ACQUISITION_ERAS for each row begin if :NEW.ACQUISITION_ERA_ID is null then select SEQ_AQE.nextval into :NEW.ACQUISITION_ERA_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER PE_TRIG before insert on PROCESSING_ERAS for each row begin if :NEW.PROCESSING_ERA_ID is null then select SEQ_PE.nextval into :NEW.PROCESSING_ERA_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER BLST_TRIG before insert on BLOCK_SITES for each row begin if :NEW.BLOCK_SITE_ID is null then select SEQ_BLST.nextval into :NEW.BLOCK_SITE_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER MR_TRIG before insert on MIGRATION_REQUESTS for each row begin if :NEW.MIGRATION_REQUEST_ID is null then select SEQ_MR.nextval into :NEW.MIGRATION_REQUEST_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER MB_TRIG before insert on MIGRATION_BLOCKS for each row begin if :NEW.MIGRATION_BLOCK_ID is null then select SEQ_MB.nextval into :NEW.MIGRATION_BLOCK_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER CS_TRIG before insert on COMPONENT_STATUS for each row begin if :NEW.COMP_STATUS_ID is null then select SEQ_CS.nextval into :NEW.COMP_STATUS_ID from dual; end if; end;
 /
