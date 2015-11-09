from __future__ import print_function
#DBS-3 imports
from dbs.apis.dbsClient import *
url="https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader/"
# API Object    
dbs3api = DbsApi(url=url)
file="/store/mc/Summer09/TTbar/GEN-SIM-RECO/MC_31X_V3-v1/0025/B2317F4D-DC88-DE11-9241-001CC4BE0A46.root"
print(dbs3api.listFileParents(logical_file_name=file))
try:
    dbs3api.listFileParents()
except Exception as e:
    print (e)

try:
    dbs3api.listBlocks(origin_site_name='FNAL')
except Exception as e:
    print (e)

