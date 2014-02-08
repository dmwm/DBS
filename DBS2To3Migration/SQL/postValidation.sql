--These sqls insert a fake acquisition era and processing era. So each DBS2 dataset will have them.
--Theses are required by DBS3 and migration.

insert into ACQUISITION_ERAS (ACQUISITION_ERA_NAME, start_date) values('DBS2_UNKNOWN_ACQUISION_ERA', 0);

insert into processing_eras(processing_version) values(0);

commit; 


update datasets set ACQUISITION_ERA_ID=(select ACQUISITION_ERA_ID from ACQUISITION_ERAS where
ACQUISITION_ERA_NAME ='DBS2_UNKNOWN_ACQUISION_ERA')     
where ACQUISITION_ERA_ID is NULL;

commit;

update datasets set PROCESSING_ERA_ID=(select PROCESSING_ERA_ID from processing_eras where
processing_version=0)   
where PROCESSING_ERA_ID is NULL;

commit;
