#DBS-3 imports
from dbs.apis.dbsClient import *
url="http://cmssrv18.fnal.gov:8585/dbs3"
# API Object    
dbs3api = DbsApi(url=url)
block="/TTbar/Summer09-MC_31X_V3-v1/GEN-SIM-RAW#a95d0528-92de-4e3c-96e4-a7ab9f59328e"
print dbs3api.listBlockChildren(block_name=block)

