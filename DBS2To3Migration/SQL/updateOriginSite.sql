alter session set NLS_DATE_FORMAT='yyyy/mm/dd:hh:mi:ssam';
set serveroutput on size 30000;
select sysdate from dual;
DECLARE
	bid2 number;
	sename varchar2(200);
	--bname2 varchar2(500);
	--bid number;
	--bname varchar2(500);
	CURSOR BKCursor IS

		select  B1.id as bid2, S1.SENAME as origin_site_name2
		from  CMS_DBS_PH_ANALYSIS_02_COPY.BLOCK B1,
		CMS_DBS_PH_ANALYSIS_02_COPY.STORAGEELEMENT S1,
		CMS_DBS_PH_ANALYSIS_02_COPY.SEBLOCK SB1
		where B1.ID=SB1.BLOCKID and S1.ID=SB1.SEID and
		B1.ID in
		(select B.ID from CMS_DBS_PH_ANALYSIS_02_COPY.BLOCK B, CMS_DBS_PH_ANALYSIS_02_COPY.STORAGEELEMENT S, 
			CMS_DBS_PH_ANALYSIS_02_COPY.SEBLOCK SB where B.ID=SB.BLOCKID and S.ID=SB.SEID
			group by B.ID having count(SB.ID)=1
		)
		;
BEGIN
	dbms_output.Put_line('hello');
	OPEN BKCursor;
	LOOP
		FETCH BKCursor INTO bid2, sename;
		EXIT WHEN BKCursor%NOTFOUND;
		update blocks set origin_site_name = sename where block_id=bid2;
		--select block_id,  block_name into bid, bname from  
		--blocks where block_name=bname2;
		--dbms_output.Put_line(bid|| ' , '||bid2 ||' ,' ||sename || ',' || bname);
	END LOOP;
	close BKCursor;
END;
/


commit;
select sysdate from dual;
