#DBS-3 imports
from dbs.apis.dbsClient import *
url="http://cmssrv18.fnal.gov:8585/dbs3"
# API Object    
dbs3api = DbsApi(url=url)
file="/store/mc/Summer09/TTbar/GEN-SIM-RECO/MC_31X_V3-v1/0025/B2317F4D-DC88-DE11-9241-001CC4BE0A46.root"
print dbs3api.listFileParents(lfn=file)

