from __future__ import print_function
#DBS-3 imports
import time
from dbs.apis.dbsClient import *
url="https://cmsweb.cern.ch/dbs/prod/global/DBSReader/"
#url="https://dbs3-test1.cern.ch/dbs/dev/global/DBSReader/"
# API Object    
dbs3api = DbsApi(url=url)

lfn = '/store/relval/CMSSW_9_2_3_patch2/DoubleEG/RECO/2017_07_11_19_22_PRref_92X_dataRun2_Prompt_RefGT_week28_2017-v1/00000/38407492-6366-E711-9039-0025905A6092.root'

flist = ['/store/relval/CMSSW_9_3_0_pre2/RelValMinBias_13/GEN-SIM-RECO/92X_upgrade2017_design_IdealBS_v7-v1/00000/FC2FFB7B-BF68-E711-9779-0CC47A4D75EC.root','/store/relval/CMSSW_9_3_0_pre2/RelValMinBias_13/GEN-SIM-RECO/92X_upgrade2017_design_IdealBS_v7-v1/00000/388DDB84-BF68-E711-BB04-0CC47A4D76D2.root','/store/relval/CMSSW_9_2_3_patch2/DoubleEG/RECO/2017_07_11_19_22_PRref_92X_dataRun2_Prompt_RefGT_week28_2017-v1/00000/B6DEF099-6366-E711-94F9-0025905A6104.root']

print(dbs3api.listFileArray(detail=1,logical_file_name=flist))

print(dbs3api.listFileArray(detail=1,logical_file_name=lfn))

print(dbs3api.listFileArray(detail=1,logical_file_name=lfn, run_num=297723, lumi_list=[127]))

print(dbs3api.listFileArray(detail=1,logical_file_name=lfn, run_num=297723, lumi_list=[127], sumOverLumi=1))

