#!/bin/python
"""
This utility function simpoly goes through the 'web' interface 
and generates a list of URLs/APIs that are supported by DBS
in Twiki format.
"""

import os
biz_path="/home/anzar/devDBS3/DBS/DBS3/Server/Python/src/dbs/business"
#print biz_path


def makedoc(param):
    ret=""
    for item in param.split("_"):
	ret += item[0].upper()+item[1:] + " "
    return ret


biz_files=os.listdir(biz_path)
#print biz_files
print "%TOC%"
print "---++++WRITE APIs"
print "Write URI use POST method and accepts a JSON object with following keys"
print ""

for abiz_file in biz_files:
    if abiz_file=='CVS' : continue
    if abiz_file=='__init__.py' : continue
    if not abiz_file.endswith(".py") : continue
    thisfilekeys=[]
    funcName=""
    stop=0
#print "THIS File : %s" % abiz_file
    for aline in open(os.path.join(biz_path, abiz_file), "r").readlines() :
	    aline=aline.strip()
	    if aline.startswith("#"): continue
	    if aline.startswith("def") and stop==1:
		stop=0
		break
	    if aline.startswith("def insert"):
		funcName=aline.split("def")[1].split("(")[0].strip()
		stop=1
	    if stop==0: continue
	    if aline.find("[") != -1:
		if aline.find("]") != -1:
		    #if aline.find("creation_date") != -1 or aline.find("create_by") != -1 : continue
		    key=aline.split("[")[1].split("]")[0].replace('"', '').strip()
		    key=key.replace("'", '').strip()
		    if key not in thisfilekeys:
			if key in ['last_modification_date', 'last_modified_by', 'creation_date', 'create_by'] : continue
			if key.endswith("_id") : continue
			if key in ([], '', '0', 0) : continue
			if len(key) < 3: continue
			thisfilekeys.append(key)
    if not thisfilekeys==[]:
	print "---+++ %s :" %funcName 
	count = 0
	nono = 0
	for akey in thisfilekeys:
	    print "   * %s : %s" %(akey, makedoc(akey))
	print "\n"
print "Above generated list needs to be manually updated, there are flaws"

