#DBS-3 imports
from dbs.apis.dbsClient import *
url="http://cmssrv18.fnal.gov:8585/dbs3"
# API Object    
dbs3api = DbsApi(url=url)
block="/TTbar/Summer09-MC_31X_V3-v1/GEN-SIM-RECO#017db322-13f2-408b-9590-1c9d8498150b"
print dbs3api.listBlockParents(block_name=block)

