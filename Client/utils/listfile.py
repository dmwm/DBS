from __future__ import print_function
#DBS-3 imports
import time
from dbs.apis.dbsClient import *
#url="https://cmsweb.cern.ch/dbs/prod/global/DBSReader/"
url="https://dbs3-test1.cern.ch/dbs/dev/global/DBSReader/"
# API Object    
dbs3api = DbsApi(url=url)

print (dbs3api.listFiles(logical_file_name="/store/mc/Summer09/TTbar/GEN-SIM-RAW/MC_31X_V3-v1/0025/F4A93663-6988-DE11-8450-003048C559C4.root"))
print (dbs3api.listFiles(dataset="/TkCosmics38T/Summer09-STARTUP31X_V3_SuperPointing-v1/RAW-RECO"))
print (dbs3api.listFiles(dataset="/TTbar/Summer09-MC_31X_V3-v1/GEN-SIM-RAW"))
print(dbs3api.listFiles(block_name="/TTbar/Summer09-MC_31X_V3-v1/GEN-SIM-RAW#f99b4c09-a68f-4e73-8f4c-560c1fa922fc"))
