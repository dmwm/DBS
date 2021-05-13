
#DBS-3 imports
import time
from dbs.apis.dbsClient import *
url="https://cmsweb.cern.ch/dbs/prod/global/DBSReader/"
#url="https://dbs3-test2.cern.ch/dbs/dev/global/DBSReader/"
# API Object    
dbs3api = DbsApi(url=url)

# will throw error because no lfn or block_name provided
run_num = 297723
#print (dbs3api.listFileLumis(run_num=run_num))

lfn = '/store/relval/CMSSW_9_2_3_patch2/DoubleEG/RECO/2017_07_11_19_22_PRref_92X_dataRun2_Prompt_RefGT_week28_2017-v1/00000/B6DEF099-6366-E711-94F9-0025905A6104.root'
#print (dbs3api.listFileLumis(logical_file_name=lfn))

#print (dbs3api.listFileLumis(block_name="/DoubleEG/CMSSW_9_2_3_patch2-2017_07_11_19_22_PRref_92X_dataRun2_Prompt_RefGT_week28_2017-v1/RECO#69d88304-6678-11e7-ab2c-02163e00d7b3"))

# We are testing listFileLumiArray 
lfn_list = ['/store/relval/CMSSW_9_3_0_pre2/RelValMinBias_13/GEN-SIM-RECO/92X_upgrade2017_design_IdealBS_v7-v1/00000/FC2FFB7B-BF68-E711-9779-0CC47A4D75EC.root',
'/store/relval/CMSSW_9_3_0_pre2/RelValMinBias_13/GEN-SIM-RECO/92X_upgrade2017_design_IdealBS_v7-v1/00000/388DDB84-BF68-E711-BB04-0CC47A4D76D2.root',
'/store/relval/CMSSW_9_2_3_patch2/DoubleEG/RECO/2017_07_11_19_22_PRref_92X_dataRun2_Prompt_RefGT_week28_2017-v1/00000/B6DEF099-6366-E711-94F9-0025905A6104.root']

print(dbs3api.listFileLumiArray(logical_file_name=lfn_list, validFileOnly=0))

#print(dbs3api.listFileLumiArray(run_num=[297723, 100], logical_file_name=lfn, validFileOnly=0))


# will throw exception because cannot be two list.
#print(dbs3api.listFileLumiArray(run_num=[297723, 100], logical_file_name=lfn_list, validFileOnly=0))

# will throw exceprion because block_name is not supported.
#print(dbs3api.listFileLumiArray(block_name="/DoubleEG/CMSSW_9_2_3_patch2-2017_07_11_19_22_PRref_92X_dataRun2_Prompt_RefGT_week28_2017-v1/RECO#69d88304-6678-11e7-ab2c-02163e00d7b3", validFileOnly=0))
