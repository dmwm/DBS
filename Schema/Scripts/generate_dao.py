import os

def makeMethodNameOld(str):

        if str.find('_') != -1:
                str=str.split('_')[0]+str[str.find("_")+1].upper()+str[str.find("_")+2:].lower()
                str=makeMethodName(str)
        return str

def makeMethodName(str):

        ret = ''
        toks=str.split('_')
        for atok in toks:
                if atok in ('Id', 'ID', 'id') : ret += 'ID'
                elif atok in ('Ds', 'DS', 'ds') : ret += 'DS'
                else: ret += atok[0].upper()+atok[1:].lower()
        if len(toks) <= 0: ret=str
        return ret

def makeVarName(str):

        ret = ''
        toks=str.split('_')
	k=0
        for atok in toks:
                if atok in ('Id', 'ID', 'id') : ret += 'ID'
                elif atok in ('Ds', 'DS', 'ds') : ret += 'DS'
                else: ret += atok[0].upper()+atok[1:].lower()
		if k==0: # levale the first letter of first part as lowe case
			k=1
			ret = atok.lower()
			continue
        if len(toks) <= 0: ret=str
        return ret 



# ==========START >>>>>>>>>>>>>

typ_map={'String':'String', 'int':'int', 'CLOB':'String', 'float' : 'float'}
typ_val_map={'String':'null', 'int': '0', 'float' : '0', 'CLOB':'null'}

lines=open("../DDL/create-oracle-schema.sql", "r").readlines()
tablefound=0
table=""
col_list=[]

# Start processing the lines from teh DDL
for aline in lines:
	if aline.strip().startswith("CREATE TABLE"):
		table=aline.strip().split()[2]
		tablefound=1
		continue
	if tablefound==1 and aline.strip().startswith(");"):
		if table.find('_') != -1:
			classname=table[0].upper()+table[1:].split('_')[0].lower()+ \
						table[table.find("_")+1].upper()+table[table.find("_")+2:].lower()
		else : classname=table[0].upper()+table[1:].lower()
		if classname.find('_') != -1:
			if classname.split('_')[0] == 'PrimaryDs':
				classname='PrimaryDS'+classname[classname.find("_")+1].upper()+\
								classname[classname.find("_")+2:].lower() 
			else:
				classname=classname.split('_')[0]+classname[classname.find("_")+1].upper()+ \
								classname[classname.find("_")+2:].lower()
		classname=classname[:len(classname)-1]

		####Write down class header

		dao_path="DAO/"+classname
		print "classname::: "+classname
		try: 
			os.mkdir(dao_path)
		except os.error , e:
			if str(e).find ('File exists') != -1 : pass
			else : raise e

		lfile=open(dao_path+"/List.py", "w")
		ifile=open(dao_path+"/Insert.py", "w")	
		ufile=open(dao_path+"/Update.py", "w")	
		dfile=open(dao_path+"/Delete.py", "w")	

		header  = "# DAO Object for %s table" % classname 
		header += "\n# $Revision: 1.1 $"
		header += "\n# $Id: generate_dao.py,v 1.1 2009/10/07 20:14:33 afaq Exp $"
		header += "\n\nfrom WMCore.Database.DBFormatter import DBFormatter"

		lfile.write(header) 
		ifile.write(header) 
		ufile.write(header) 
		dfile.write(header)

                isql="INSERT INTO "+table.upper()+"("
                isqlvals=""
                ssql="SELECT "
                idx=0
		binds=""
                for acol in col_list:
			binds+= ", "+ makeVarName(acol[0]) + " = " + "0"
			
                        if idx==0:
                                isql+= acol[0]
                                isqlvals+= "?"
                                ssql+=acol[0]+" AS C"+acol[0]
                                idx=1
                        else:
                                isql+= ", "+acol[0]
                                isqlvals+= ", ?"
                                ssql+=", "+acol[0]+" AS C"+acol[0]

                isql+=") VALUES ("+isqlvals+");"
                ssql+=" FROM "+table;

		ifile.write("\nclass List(DBFormatter):")
		ifile.write("\n\n    sql = \"\"\""+isql+"\"\"\"" )
		ifile.write("\n    def getBinds(self"+binds+ " ):" )
		ifile.write("\n    	binds = {}")
		ifile.write("\n    def execute(self"+binds+ " ):" )
                ifile.write("\n            binds = self.getBinds("+binds+")")
                ifile.write("\n            result = self.dbi.processData(self.sql, binds,")
                ifile.write("\n                             conn = conn, transaction = transaction)")
                ifile.write("\n            return")

                #ifile.write(isql)
                lfile.write(ssql)

		lfile.close()
		ifile.close()
		ufile.close()
		dfile.close()	

		table=""
		col_list=[]
		tablefound=0

	#CONSTRAINT PK_AF PRIMARY KEY (ASSOCATED_FILE_ID)
	#if aline.strip().startswith('CONSTRAINT') and  aline.strip().find("PRIMARY KEY") !=-1:
	#	continue

	####This if block is collecting columns and types
	if tablefound == 1 and not aline.strip().startswith("("):
		if aline.strip().startswith(');') : continue
		if aline.strip().startswith('GRANT') : continue
		if aline.strip().startswith('ALTER') : continue
		if aline.strip().startswith('FOREIGN') : continue
		if aline.strip().startswith('/*') : continue
		if aline.strip().startswith('CREATE INDEX') : continue
		if aline.strip().startswith('CONSTRAINT') : continue

		if aline.strip() in ('') : continue
		col=aline.split()[0]
		typ=aline.split()[1]
		if typ.startswith('CLOB'): typ="String"
		if typ.startswith('INTEGER') : typ="int"
		if typ.startswith('VARCHAR') : typ="String"
		if typ.startswith('NUMBER') : typ="int"
		if typ.startswith('FLOAT') : typ="float"
		col_list.append((col, typ))

