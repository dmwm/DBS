#DBS-3 imports
from dbs.apis.dbsClient import *
url="http://cmssrv18.fnal.gov:8585/dbs3"
# API Object    
dbs3api = DbsApi(url=url)
file="/store/mc/Summer09/TTbar/GEN-SIM-RAW/MC_31X_V3-v1/0025/BEA32349-DC88-DE11-A7D9-001CC4BDFB44.root"
print dbs3api.listFileChildren(lfn=file)

