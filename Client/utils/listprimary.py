#DBS-3 imports
from dbs.apis.dbsClient import *
url="http://cmssrv48.fnal.gov:8989/DBSServlet"
# API Object    
dbs3api = DbsApi(url=url)
# Is service Alive
print dbs3api.ping()
print dbs3api.listPrimaryDataset("ZeeJet_Pt230to300")
