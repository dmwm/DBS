#The below script has to be run in order to prepare the DB for deployment tests.
#The 'TEST' and 'PROCESSING' are not part of offical DBS dataset types. They are only used for tests.

insert 
when not exists (select dataset_access_type from DBOWNER.dataset_access_types where dataset_access_type='TEST') THEN
into  DBOWNER.dataset_access_types( dataset_access_type) 
values ('TEST')
select 'TEST' from dual;
/
insert 
when not exists (select dataset_access_type from DBOWNER.dataset_access_types where dataset_access_type='PROCESSING') THEN
into  DBOWNER.dataset_access_types( dataset_access_type) 
values ('PROCESSING')
select 'PROCESSING' from dual;
/

commit;
/

