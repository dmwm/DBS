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

print "---++++READ APIs (GET URIs)"
print "| <b>API</b> | <b>Base URI</b> | <b>Supported Parameters</b> |"
for aline in reader_web:
    aline=aline.strip()
    if aline.startswith("self.addService"):
	api=aline.split(',')[1].replace("'", "").strip()
	api_url= "%s/%s" % (base_url, api)
	params=[]
	if len(aline.split('[')) > 1:
	    params=aline.split('[')[1].split(']')[0].replace("'", "").split(",")
	print "| *%s* | *%s* | %s |" % ( api, api_url, "*Possible URIs*" )
	count=0
	last_param=""
	anded=""
	toprint=""
	if len(params) > 0:
	    toprint = "|  |  | "
	for aparam in params:
	    if count !=0:
		anded="%s&%s=<%s>" % (last_param, aparam.strip(), aparam.strip())
	    else:
		anded="%s=<%s>" % (aparam.strip(), aparam.strip())
	    param_uri = "%s?%s" %(api_url, anded.strip())
	    toprint += "<verbatim> %s </verbatim>" % param_uri	    
	    last_param=anded
	    count+=1
	if len(params) > 0:
	    toprint += " | "
	    print toprint
	    
print "\n\n---++++WRITE APIs (POST URIs)"
print "DBS uses POST instead of PUT, as in some cases the payload size can be larger than PUT payload's upper limit"
print "| <b>API</b> | <b>URI</b> |"
for aline in writer_web:
    aline=aline.strip()
    if aline.startswith("self.addService") and aline.find('POST') != -1 :
	api=aline.split(',')[1].replace("'", "").strip()
	api_url= "%s/%s" % (base_url, api)
	print " | %s | %s |" % ( api,  api_url)

