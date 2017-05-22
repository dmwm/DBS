
## This is a simple example to load a block dump file into DBS3.
## One can use blockdump.dict as an example to see what is the basic requirement to load
## a block into DBS3.
##
import os, sys, imp
import cjson
import pprint
import ast 
from dbs.apis.dbsClient import *
"""
export DBS_WRITER_URL="https://dbs3-test1.cern.ch/dbs/dev/global/DBSWriter"

"""
url=os.environ['DBS_WRITER_URL']
# API Object    
dbs3api = DbsApi(url=url)
input = sys.argv[1]
print(input)
try:
    infofile=open(input, "r")
    indata = infofile.read()
    id=indata.find("=")
    d = indata[(id+1):]
    d2 = ast.literal_eval(d)  
    dbs3api.insertBulkBlock(d2)
    infofile.close()
except Exception as e:
    print(e)
