#DBS-3 imports
from dbs.apis.dbsClient import *
url="http://cmssrv48.fnal.gov:8989/DBSServlet"
# API Object    
dbs3api = DbsApi(url=url)
# Is service Alive
print dbs3api.ping()
# All datasets, NOT implemented yet in server
print dbs3api.listDatasets()

#print dbs3api.listDataset("/ZeeJet_Pt230to300/Summer09-MC_31X_V3_7TeV-v1/GEN-SIM-RAW")
#print dbs3api.listDataset("/Wmunu_Wplus-powheg/Summer09-MC_31X_V3_7TeV_MCDB-v1/USER")
print "\n"
print dbs3api.listDataset("/TTbar/Summer09-MC_31X_V3-v1/GEN-SIM-RAW")
print dbs3api.listDataset("/TkCosmics38T/Summer09-STARTUP31X_V3_SuperPointing-v1/RAW-RECO'")
