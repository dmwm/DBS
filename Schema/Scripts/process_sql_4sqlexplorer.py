import os
lines=open("../DDL/DBS3-Oracle.sql", "r").readlines()
tablefound=0
table=""
col_list=[]
inserts=[]
selects=[]
trigs=[]

trig_tbl_map = {"ACQUISITION_ERAS" : "AQE",
"APPLICATION_EXECUTABLES": "AE" ,
"ASSOCIATED_FILES": "AF" ,
"BLOCKS": "BK" ,
"BLOCK_PARENTS": "BP" ,
"BLOCK_STORAGE_ELEMENTS": "BSE" ,
"BRANCH_HASHES": "BH" ,
"DATA_TIERS": "DT" ,
"DBS_VERSIONS": "DV", 
"FILES": "FL" ,
"FILE_LUMIS": "FLM" ,
"FILE_PARENTS": "FP" ,
"FILE_PROCESS_CONFIGS": "FPC" ,
"FILE_TYPES": "FT" ,
"PARAMETER_SET_HASHES": "PSH" ,
"PATHS": "PH" ,
"PATH_PARENTS": "PP" ,
"PATH_PROCESS_CONFIGS": "PPC" ,
"PATH_RUNS": "PR" ,
"PATH_TYPES": "PT" ,
"PHYSICS_GROUPS": "PG" ,
"PRIMARY_DATASETS": "PDS" ,
"PRIMARY_DS_TYPES": "PDT" ,
"PROCESSED_DATASETS": "PSDS" ,
"PROCESSING_ERAS": "PE" ,
"PROCESS_CONFIGURATIONS": "PC" ,
"RELEASE_VERSIONS": "RV" ,
"SITES": "SI" ,
"STORAGE_ELEMENTS": "SE" ,
}

tbl_pk_map={}

for aline in lines:
	if aline.strip().startswith("CREATE TABLE"):
		import pdb
		pdb.set_trace()
		tablename=aline.strip().split('.')[1].replace ('"', '')	
		trig="\nCREATE OR REPLACE TRIGGER %s_TRIG before insert on %s for each row begin if :NEW.__PK__ is null then select seq_%s.nextval into :NEW.__PK__ from dual; end if; end;\n /" % (trig_tbl_map[tablename], tablename, trig_tbl_map[tablename])

		trigs.append( (tablename, trig) )
		tablefound=1
		table=aline.strip().split(".")[1].replace('"', '')
		continue

	if tablefound==1 and aline.strip().startswith(") ;"):
		isql="INSERT INTO "+table.upper()+"("
		isqlvals=""
		ssql="SELECT "

		idx=0
		for acol in col_list:
			
			if idx==0:
				isql+= acol
				isqlvals+= "?"
				ssql+=acol+" AS C"+acol
				idx=1
			else: 	
				isql+= ", "+acol
				isqlvals+= ", ?"
				ssql+=", "+acol+" AS C"+acol
			
		isql+=") VALUES ("+isqlvals+");"
		ssql+=" FROM "+table;
		inserts.append(isql)
		selects.append(ssql)

		table=""
		col_list=[]
		tablefound=0


	if tablefound == 1 and not aline.strip().startswith("("):
		col=aline.replace(")", "").split()[0].replace('"', '').strip()
		#col_list.append(aline.replace(")", "").split()[0].replace('"', '').strip())
		col_list.append(col)

	if aline.strip().startswith("ALTER TABLE") and  aline.strip().find("PRIMARY KEY") !=-1:
		#ALTER TABLE "CMS_DBS3_OWNER"."STORAGE_ELEMENTS" ADD CONSTRAINT "PK_SE" PRIMARY KEY ("SE_ID") ENABLE;
		tbl=aline.split(".")[1].split("ADD CONSTRAINT")[0].replace('"', '').strip()
		col=aline.split("PRIMARY KEY (")[1].split(")")[0].replace('"', '').strip()
		tbl_pk_map[tbl]=col


###### Generate INSERT Statements

#for x in inserts: print x
#print "\n\n"

###### Generate SELECT Statements

#for y in selects: print y

###### Generate AUTO INC Triggers

for (t,z) in trigs : 
	print z.replace( "__PK__", tbl_pk_map[t] )
