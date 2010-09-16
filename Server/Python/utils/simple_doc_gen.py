#!/bin/python
"""
This utility function simpoly goes through the 'web' interface 
and generates a list of URLs/APIs that are supported by DBS
in Twiki format.
"""

import os

web_path="/uscms/home/anzar/devDBS3/DBS/DBS3/Server/Python/src/dbs/web"
reader_web=open(os.path.join(web_path, "DBSReaderModel.py"), "r").readlines()
writer_web=open(os.path.join(web_path, "DBSWriterModel.py"), "r").readlines()
base_url="http://....../dbs3"

def makedoc(param):
        ret=""
	for item in param.split("_"):
	    ret += item[0].upper()+item[1:] + " "
	return ret
			    


apilist={}
for aline in reader_web:
    aline=aline.strip()
    if aline.startswith("def"):
	api= aline.split('def')[1].strip().split('(')[0]
	if api not in apilist.keys():
	    apilist[api]=[]
	api_url= "%s/%s" % (base_url, api)
	params=[]
	toks = aline.split('def')[1].strip().split('(')[1].split('):')
	if len(toks) > 1:
	    for atok in toks:
		atok = atok.strip()
		if atok in ('', 'self'): continue
		params.append(atok)
	if len(params) > 0:
	    apilist[api]=params

print apilist	    
print "---++++READ APIs"
for api, params in apilist.items():
    print "\n---++++ %s" %api
    for aparam in params:
	print "   * %s : %s" % (aparam, makedoc(aparam))


