from __future__ import print_function
import sys
import time

#DBS-3 imports
from dbs.apis.dbsClient import *
try:
        # Python 2.6
        import json
except:
        # Prior to 2.6 requires simplejson
        import simplejson as json

#url="https://cmsweb.cern.ch/dbs/prod/global/DBSReader/"
url="https://dbs3-test1.cern.ch/dbs/dev/global/DBSReader/"
if len(sys.argv) < 1:
    print("Usage: python %s <block>" %sys.argv[0])
    sys.exit(1)
		
# API Object    
dbs3api = DbsApi(url=url)
thisBlock = sys.argv[1]
#print thisBlock
# Is service Alive
#print dbs3api.ping()
#blocks= dbs3api.listBlock("/TkCosmics38T/Summer09-STARTUP31X_V3-v1/GEN-SIM-DIGI-RAW#5ca8fd67-4bc1-4d24-a88c-f2dbf864793b")
start_time=time.time()
files = dbs3api.listFiles(block_name=thisBlock)
print(files)
#
