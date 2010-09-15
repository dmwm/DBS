
CREATE OR REPLACE TRIGGER AQE_TRIG before insert on ACQUISITION_ERAS for each row begin if :NEW.ACQUISITION_ERA_ID is null then select seq_AQE.nextval into :NEW.ACQUISITION_ERA_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER AE_TRIG before insert on APPLICATION_EXECUTABLES for each row begin if :NEW.APP_EXEC_ID is null then select seq_AE.nextval into :NEW.APP_EXEC_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER AF_TRIG before insert on ASSOCIATED_FILES for each row begin if :NEW.ASSOCATED_FILE_ID is null then select seq_AF.nextval into :NEW.ASSOCATED_FILE_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER BK_TRIG before insert on BLOCKS for each row begin if :NEW.BLOCK_ID is null then select seq_BK.nextval into :NEW.BLOCK_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER BP_TRIG before insert on BLOCK_PARENTS for each row begin if :NEW.BLOCK_PARENT_ID is null then select seq_BP.nextval into :NEW.BLOCK_PARENT_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER BSE_TRIG before insert on BLOCK_STORAGE_ELEMENTS for each row begin if :NEW.BLOCK_SE_ID is null then select seq_BSE.nextval into :NEW.BLOCK_SE_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER BH_TRIG before insert on BRANCH_HASHES for each row begin if :NEW.BRANCH_HASH_ID is null then select seq_BH.nextval into :NEW.BRANCH_HASH_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER DT_TRIG before insert on DATA_TIERS for each row begin if :NEW.DATA_TIER_ID is null then select seq_DT.nextval into :NEW.DATA_TIER_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER DV_TRIG before insert on DBS_VERSIONS for each row begin if :NEW.DBS_VERSION_ID is null then select seq_DV.nextval into :NEW.DBS_VERSION_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER FL_TRIG before insert on FILES for each row begin if :NEW.FILE_ID is null then select seq_FL.nextval into :NEW.FILE_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER FLM_TRIG before insert on FILE_LUMIS for each row begin if :NEW.FILE_LUMI_ID is null then select seq_FLM.nextval into :NEW.FILE_LUMI_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER FP_TRIG before insert on FILE_PARENTS for each row begin if :NEW.FILE_PARENT_ID is null then select seq_FP.nextval into :NEW.FILE_PARENT_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER FPC_TRIG before insert on FILE_PROCESS_CONFIGS for each row begin if :NEW.FILE_PROCESS_CONFIG_ID is null then select seq_FPC.nextval into :NEW.FILE_PROCESS_CONFIG_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER FT_TRIG before insert on FILE_TYPES for each row begin if :NEW.FILE_TYPE_ID is null then select seq_FT.nextval into :NEW.FILE_TYPE_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER PSH_TRIG before insert on PARAMETER_SET_HASHES for each row begin if :NEW.PARAMETER_SET_HASH_ID is null then select seq_PSH.nextval into :NEW.PARAMETER_SET_HASH_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER PH_TRIG before insert on PATHS for each row begin if :NEW.PATH_ID is null then select seq_PH.nextval into :NEW.PATH_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER PP_TRIG before insert on PATH_PARENTS for each row begin if :NEW.PATH_PARENT_ID is null then select seq_PP.nextval into :NEW.PATH_PARENT_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER PPC_TRIG before insert on PATH_PROCESS_CONFIGS for each row begin if :NEW.PATH_PROCESS_CONF_ID is null then select seq_PPC.nextval into :NEW.PATH_PROCESS_CONF_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER PR_TRIG before insert on PATH_RUNS for each row begin if :NEW.PATH_RUN_ID is null then select seq_PR.nextval into :NEW.PATH_RUN_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER PT_TRIG before insert on PATH_TYPES for each row begin if :NEW.PATH_TYPE_ID is null then select seq_PT.nextval into :NEW.PATH_TYPE_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER PG_TRIG before insert on PHYSICS_GROUPS for each row begin if :NEW.PHYSICS_GROUP_ID is null then select seq_PG.nextval into :NEW.PHYSICS_GROUP_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER PDS_TRIG before insert on PRIMARY_DATASETS for each row begin if :NEW.PRIMARY_DS_ID is null then select seq_PDS.nextval into :NEW.PRIMARY_DS_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER PDT_TRIG before insert on PRIMARY_DS_TYPES for each row begin if :NEW.PRIMARY_DS_TYPE_ID is null then select seq_PDT.nextval into :NEW.PRIMARY_DS_TYPE_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER PSDS_TRIG before insert on PROCESSED_DATASETS for each row begin if :NEW.PROCESSED_DS_ID is null then select seq_PSDS.nextval into :NEW.PROCESSED_DS_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER PE_TRIG before insert on PROCESSING_ERAS for each row begin if :NEW.PROCESSING_ERA_ID is null then select seq_PE.nextval into :NEW.PROCESSING_ERA_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER PC_TRIG before insert on PROCESS_CONFIGURATIONS for each row begin if :NEW.PROCESS_CONFIG_ID is null then select seq_PC.nextval into :NEW.PROCESS_CONFIG_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER RV_TRIG before insert on RELEASE_VERSIONS for each row begin if :NEW.RELEASE_VERSION_ID is null then select seq_RV.nextval into :NEW.RELEASE_VERSION_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER SI_TRIG before insert on SITES for each row begin if :NEW.SITE_ID is null then select seq_SI.nextval into :NEW.SITE_ID from dual; end if; end;
 /

CREATE OR REPLACE TRIGGER SE_TRIG before insert on STORAGE_ELEMENTS for each row begin if :NEW.SE_ID is null then select seq_SE.nextval into :NEW.SE_ID from dual; end if; end;
 /
