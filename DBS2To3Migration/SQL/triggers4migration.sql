CREATE OR REPLACE TRIGGER AQE_TRIG before insert on ACQUISITION_ERAS for each row begin if
:NEW.ACQUISITION_ERA_ID is null then select SEQ_AQE.nextval into :NEW.ACQUISITION_ERA_ID from dual; end
if; end;
 /
 CREATE OR REPLACE TRIGGER PSDS_TRIG before insert on PROCESSED_DATASETS for each row begin if
 :NEW.PROCESSED_DS_ID is null then select SEQ_PSDS.nextval into :NEW.PROCESSED_DS_ID from dual; end if;
 end;
  /
