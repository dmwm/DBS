--run the alter command in sqlplus.
alter session set NLS_DATE_FORMAT='yyyy/mm/dd:hh:mi:ssam';

spool on-updateOriginSite..txt;
select sysdate from dual;

update blocks B3 set B3.ORIGIN_SITE_NAME = (
select  S1.SENAME as origin_site_name2 
from  CMS_DBS_PH_ANALYSIS_02_COPY.BLOCK B1,
CMS_DBS_PH_ANALYSIS_02_COPY.STORAGEELEMENT S1, 
CMS_DBS_PH_ANALYSIS_02_COPY.SEBLOCK SB1
where B1.ID=SB1.BLOCKID and S1.ID=SB1.SEID and 
B1.ID in (select B.ID from CMS_DBS_PH_ANALYSIS_02_COPY.BLOCK B, CMS_DBS_PH_ANALYSIS_02_COPY.STORAGEELEMENT S, CMS_DBS_PH_ANALYSIS_02_COPY.SEBLOCK SB
where B.ID=SB.BLOCKID and S.ID=SB.SEID
group by B.ID having count(SB.ID)=1)
and b3.block_name = b1.NAME
)
where exists (
select S1.SENAME as origin_site_name2 
from  CMS_DBS_PH_ANALYSIS_02_COPY.BLOCK B1,
CMS_DBS_PH_ANALYSIS_02_COPY.STORAGEELEMENT S1, 
CMS_DBS_PH_ANALYSIS_02_COPY.SEBLOCK SB1
where B1.ID=SB1.BLOCKID and S1.ID=SB1.SEID and 
B1.ID in (select B.ID from CMS_DBS_PH_ANALYSIS_02_COPY.BLOCK B, CMS_DBS_PH_ANALYSIS_02_COPY.STORAGEELEMENT S, CMS_DBS_PH_ANALYSIS_02_COPY.SEBLOCK SB
where B.ID=SB.BLOCKID and S.ID=SB.SEID
group by B.ID having count(SB.ID)=1)
and b3.block_name = b1.NAME
);

commit;		
select sysdate from dual;
spool off
exit;
