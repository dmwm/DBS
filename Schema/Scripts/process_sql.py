import os
lines=open("../DDL/create-oracle-schema.sql", "r").readlines()
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
"FILE_OUTPUT_MOD_CONFIGS": "FC" ,
"FILE_TYPES": "FT" ,
"PARAMETER_SET_HASHES": "PSH" ,
"DATASETS": "DS" ,
"DATASET_PARENTS": "DP" ,
"DATASET_OUTPUT_MOD_CONFIGS": "DC" ,
"DATASET_RUNS": "DR" ,
"DATASET_TYPES": "DTP" ,
"PHYSICS_GROUPS": "PG" ,
"PRIMARY_DATASETS": "PDS" ,
"PRIMARY_DS_TYPES": "PDT" ,
"PROCESSED_DATASETS": "PSDS" ,
"PROCESSING_ERAS": "PE" ,
"OUTPUT_MODULE_CONFIGS": "OMC" ,
"RELEASE_VERSIONS": "RV" ,
"SITES": "SI" ,
"STORAGE_ELEMENTS": "SE" ,
}

tbl_pk_map={}

for aline in lines:
	if aline.strip().startswith("CREATE TABLE"):
		table=aline.strip().split()[2]
		trig="\nCREATE OR REPLACE TRIGGER %s_TRIG before insert on %s for each row begin if :NEW.__PK__ is null then select SEQ_%s.nextval into :NEW.__PK__ from dual; end if; end;\n /" % (trig_tbl_map[table], table, trig_tbl_map[table])
		trigs.append( (table, trig) )
		tablefound=1
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

	#CONSTRAINT PK_AF PRIMARY KEY (ASSOCATED_FILE_ID)
	if aline.strip().startswith('CONSTRAINT') and  aline.strip().find("PRIMARY KEY") !=-1:
		col=aline.strip().split('PRIMARY KEY')[1].strip().split('(')[1].split(')')[0]
		tbl_pk_map[table]=col


	if tablefound == 1 and not aline.strip().startswith("("):
		#print aline
		if aline.strip().startswith(');') : continue
		if aline.strip().startswith('GRANT') : continue
		if aline.strip().startswith('ALTER') : continue
		if aline.strip().startswith('FOREIGN') : continue
		if aline.strip().startswith('/*') : continue
		if aline.strip().startswith('CREATE INDEX') : continue
		if aline.strip().startswith('CONSTRAINT') : continue

		if aline.strip() in ('') : continue
		col=aline.split()[0]
		col_list.append(col)


###### Generate INSERT Statements

#for x in inserts: print x
#print "\n\n"

###### Generate SELECT Statements

#for y in selects: print y

###### Generate AUTO INC Triggers

for (t,z) in trigs : 
	print z.replace( "__PK__", tbl_pk_map[t] )
