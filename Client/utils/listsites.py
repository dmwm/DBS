#DBS-3 imports
from dbs.apis.dbsClient import *
url="http://cmssrv18.fnal.gov:8585/dbs3"
# API Object    
dbs3api = DbsApi(url=url)
block="/QCD_Pt800-herwigjimmy/Summer09-MC_31X_V3_7TeV_Test_Belgium_Team1-v1/GEN-SIM-RAW#b9347d2d-cbcf-44b3-ad1f-07ef079a1adb"
block="/anzar22/Summer09-STARTUP31X_V3-v1/GEN-SIM-DIGI-RAW#1237f9d-d44e-45a6-b87b-8fe6e27011162"
print dbs3api.listSites(block_name=block)
print dbs3api.listSites()

