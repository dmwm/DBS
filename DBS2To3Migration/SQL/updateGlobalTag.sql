DECLARE
   id NUMBER;
   tag VARCHAR2(256);
 
CURSOR OMCursor IS

   select  mytable.myid as output_mod_config_id, pd.globaltag as global_tag 

from (select ct, myid from (select count(distinct globaltag) ct , al.id myid  from CMS_DBS_PROD_GLOBAL.processeddataset pd
                  join CMS_DBS_PROD_GLOBAL.PROCALGO PA on pd.id=pa.DATASET
                  join CMS_DBS_PROD_GLOBAL.ALGORITHMCONFIG AL on pa.ALGORITHM=al.ID      
                 where pd.GLOBALTAG is not null 
                 group by al.ID)  
                 where ct = 1) mytable
join CMS_DBS_PROD_GLOBAL.PROCALGO PA on PA.ALGORITHM = mytable.myid
join CMS_DBS_PROD_GLOBAL.processeddataset pd on pd.id = pa.dataset                 
where not pd.globaltag is null
;   

BEGIN
   OPEN OMCursor;
   LOOP
       FETCH OMCursor INTO id, tag;
       EXIT WHEN OMCursor%NOTFOUND;
       update output_module_configs set global_tag=tag where output_mod_config_id=id;
   END LOOP;
   CLOSE OMCursor;    
END;  


--query to verify the result
--Following two queries should give the same number of global tags inserted into the db.
--select count(myid) from 
--(                 
--select count(distinct globaltag) ct , al.id myid  from CMS_DBS_PROD_GLOBAL.processeddataset pd
--                  join CMS_DBS_PROD_GLOBAL.PROCALGO PA on pd.id=pa.DATASET
--                  join CMS_DBS_PROD_GLOBAL.ALGORITHMCONFIG AL on pa.ALGORITHM=al.ID      
--                 where pd.GLOBALTAG is not null 
--                 group by al.ID order by al.id
--) where ct =1; 


--Select count(omc.global_tag) from output_module_configs omc where omc.global_tag <>'UNKNOWN';
