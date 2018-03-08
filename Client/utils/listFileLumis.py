from __future__ import print_function
#DBS-3 imports
import time
from dbs.apis.dbsClient import *
url="https://cmsweb.cern.ch/dbs/prod/global/DBSReader/"
#url="https://dbs3-test1.cern.ch/dbs/dev/global/DBSReader/"
# API Object    
dbs3api = DbsApi(url=url)

lfn = '/store/relval/CMSSW_9_2_3_patch2/DoubleEG/RECO/2017_07_11_19_22_PRref_92X_dataRun2_Prompt_RefGT_week28_2017-v1/00000/B6DEF099-6366-E711-94F9-0025905A6104.root'
#print (dbs3api.listFileLumis(logical_file_name=lfn))

print (dbs3api.listFileLumis(block_name="/DoubleEG/CMSSW_9_2_3_patch2-2017_07_11_19_22_PRref_92X_dataRun2_Prompt_RefGT_week28_2017-v1/RECO#69d88304-6678-11e7-ab2c-02163e00d7b3"))
