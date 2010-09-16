#!/bin/python
"""
This utility function simpoly goes through the 'web' interface 
and generates a list of URLs/APIs that are supported by DBS
in Twiki format.
"""

import os

web_path="/home/anzar/devDBS3/DBS/DBS3/Server/Python/src/dbs/web"
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
    if aline.startswith("self.addService"):
	api=aline.split(',')[1].replace("'", "").strip()
	if api not in apilist.keys():
	    apilist[api]=[]
	api_url= "%s/%s" % (base_url, api)
	params=[]
	if len(aline.split('[')) > 1:
	    params=aline.split('[')[1].split(']')[0].replace("'", "").split(",")
	count=0
	last_param=""
	anded=""
	if len(params) > 0:
	    apilist[api]=params
	for aparam in params:
	    if count !=0:
		anded="%s&%s=<%s>" % (last_param, aparam.strip(), aparam.strip())
	    else:
		anded="%s=<%s>" % (aparam.strip(), aparam.strip())
#apilist[api].append(anded.strip())
	    last_param=anded
	    count+=1
	    #if len(params) > 0:
	    

print "---++++WRITE APIs (POST URIs)"
print "DBS uses POST instead of PUT, as in some cases the payload size can be larger than PUT payload's upper limit"
for aline in writer_web:
    aline=aline.strip()
    if aline.startswith("self.addService"):
	api=aline.split(',')[1].replace("'", "").strip()
	print "---+++++ %s" %api
	api_url= "%s/%s" % (base_url, api)
	print "<verbatim> %s </verbatim> " % api_url 

print "---++++READ APIs"
for api, params in apilist.items():
    print "\n---++++ %s" %api
    for aparam in params:
	print "   * %s : %s" % (aparam, makedoc(aparam))


