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



typ_map={'String':'String', 'Integer':'int', 'CLOB':'String', 'Float' : 'float'}

lines=open("../DDL/DBS3-Oracle.sql", "r").readlines()
tablefound=0
table=""
col_list=[]

for aline in lines:
	if aline.strip().startswith("CREATE TABLE"):
		table=aline.strip().split()[2]
		tablefound=1
		continue

	if tablefound==1 and aline.strip().startswith(");"):
		
		if table.find('_') != -1:
			
			classname=table[0].upper()+table[1:].split('_')[0].lower()+table[table.find("_")+1].upper()+table[table.find("_")+2:].lower()
		else : classname=table[0].upper()+table[1:].lower()

		if classname.find('_') != -1:
			if classname.split('_')[0] == 'PrimaryDs':
				classname='PrimaryDS'+classname[classname.find("_")+1].upper()+classname[classname.find("_")+2:].lower() 
			else:
				classname=classname.split('_')[0]+classname[classname.find("_")+1].upper()+classname[classname.find("_")+2:].lower()

		classname=classname[:len(classname)-1]

		header= """/**
 * 
 $Revision: 1.3 $"
 $Id: generate_dataobjs.py,v 1.3 2009/09/04 20:25:48 afaq Exp $"
 *
 * Data Object from table : %s
*/

package cms.dbs.dataobjs;

public class %s extends JSONObject {

	public %s ( ) {

	}
""" % (table, classname, classname)

		k=0
		ctor=''
		putonce=''
		getmthd=''
		for (acol, typ) in col_list:

			mthd=makeMethodName(acol)
			var=makeVarName(acol)
			getmthd+="""
	%s get%s ( ) {
		%s %s = null;
               	if (!JSONObject.NULL.equals(this.get("%s"))) {
                       	%s = (%s) this.get("%s");
               	}
                return %s;
        }
	""" % (typ_map[typ], mthd, typ_map[typ], var, acol, var, typ, acol, var)		
			# Prepare CTOR line
			if k==0 : 
				ctor += typ_map[typ]+' '+var
				k=1
			else : 	ctor += ', '+ typ_map[typ]+' '+var
			putonce += '\n                this.putOnce("%s", (%s) %s );' % ( acol, typ, var )


		ctor_stmt="""
        public %s ( %s )  {
		%s
        }
""" % ( classname , ctor, putonce )

		f=open(classname+".java", "w")
		f.write(header)
		f.write(ctor_stmt)
		f.write(getmthd)
		f.write("\n}") # close the class def
		f.close()
		table=""
		col_list=[]
		tablefound=0

	#CONSTRAINT PK_AF PRIMARY KEY (ASSOCATED_FILE_ID)
	#if aline.strip().startswith('CONSTRAINT') and  aline.strip().find("PRIMARY KEY") !=-1:
	#	continue

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
		if typ.startswith('INTEGER') : typ="Integer"
		if typ.startswith('VARCHAR') : typ="String"
		if typ.startswith('NUMBER') : typ="Integer"
		if typ.startswith('FLOAT') : typ="Float"
		col_list.append((col, typ))

