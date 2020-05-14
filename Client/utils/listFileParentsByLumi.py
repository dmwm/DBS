from __future__ import print_function
#DBS-3 imports
from dbs.apis.dbsClient import *
url="https://cmsweb.cern.ch:8443/dbs/prod/global/DBSReader/"

# API Object    
dbs3api = DbsApi(url=url)
block ="/QCD_Pt_600to800_TuneCP5_13TeV_pythia8/RunIISummer19UL18MiniAOD-106X_upgrade2018_realistic_v11_L1v1-v2/MINIAODSIM#ee83a871-c4a1-4d20-bd24-a599220e2459"
try:
    print(dbs3api.listFileParentsByLumi(block_name=block))
except Exception as e:
    print (e)
