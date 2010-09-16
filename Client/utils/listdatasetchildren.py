#DBS-3 imports
from dbs.apis.dbsClient import *
url="http://cmssrv18.fnal.gov:8585/dbs3"
# API Object    
dbs3api = DbsApi(url=url)
dataset="/TTbar/Summer09-MC_31X_V3-v1/GEN-SIM-RAW"
print dbs3api.listDatasetChildren(dataset=dataset)

