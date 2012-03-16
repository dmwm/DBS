import os, sys, imp
import cjson
from dbs.apis.dbsClient import *
#url = "http://cms-xen40.fnal.gov/cms_dbs/DBS"
#url="https://localhost:1443/dbs/int/global/DBSWriter"
url=os.environ['DBS_WRITER_URL']
# API Object    
dbs3api = DbsApi(url=url)

try:
    infofile=open("blockdump.dict","r")
    indata = infofile.readline()
    testparams = cjson.decode(indata)
    dbs3api.insertBulkBlock(testparams)
except Exception, e:
    print e
