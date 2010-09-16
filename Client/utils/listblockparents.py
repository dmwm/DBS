#DBS-3 imports
from dbs.apis.dbsClient import *
url="http://cmssrv18.fnal.gov:8585/dbs3"
# API Object    
dbs3api = DbsApi(url=url)
block="/QCD_Pt800-herwigjimmy/Summer09-MC_31X_V3_7TeV_Test_Belgium_Team2_AODSIM-v1/AODSIM#21cc1810-ed94-4317-b37c-5948f9bd543e"
print dbs3api.listBlockParents(block_name=block)

