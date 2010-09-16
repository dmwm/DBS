#DBS-3 imports
from dbs.apis.dbsClient import *
#url="http://cmssrv48.fnal.gov:8989/DBSServlet"
url="http://cmssrv18.fnal.gov:8585/dbs3"
# API Object    
dbs3api = DbsApi(url=url)
#print dbs3api.listPrimaryDatasets()
print dbs3api.insertPrimaryDataset({"PRIMARY_DS_TYPE": "test", "PRIMARY_DS_NAME": "TkCosmics38T_ANZAR003"})
# Is service Alive
"""
print dbs3api.ping()
print dbs3api.listPrimaryDataset("Wmunu_Wplus-powheg")
print dbs3api.listPrimaryDataset("ANZAR003")
print dbs3api.listPrimaryDataset("ANZAR004")
"""
