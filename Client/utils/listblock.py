#DBS-3 imports
from dbs.apis.dbsClient import *
url="http://cmssrv48.fnal.gov:8989/DBSServlet"
# API Object    
dbs3api = DbsApi(url=url)
# Is service Alive
print dbs3api.ping()
print dbs3api.listBlock("/TTbar/Summer09-MC_31X_V3-v1/GEN-SIM-RAW#f99b4c09-a68f-4e73-8f4c-560c1fa922fc")
