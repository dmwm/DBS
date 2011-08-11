import os, sys, imp
import cjson
from dbs.apis.dbsClient import *
url = "http://cms-xen40.fnal.gov/cms_dbs/DBS"
# API Object    
dbs3api = DbsApi(url=url)

try:
    infofile=open("blockdump.dict","r")
    indata = infofile.readline()
    testparams = cjson.decode(indata)
    dbs3api.insertBlockBluk(testparams)
except Exception, e:
    print e
