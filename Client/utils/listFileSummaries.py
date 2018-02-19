from __future__ import print_function
from dbs.apis.dbsClient import *
url="https://cmsweb.cern.ch/dbs/prod/global/DBSReader/"
#url="https://dbs3-test1.cern.ch/dbs/dev/global/DBSReader/"
dbs3api = DbsApi(url=url)

dataset = '/DoubleEG/CMSSW_9_2_3_patch2-2017_07_11_19_22_PRref_92X_dataRun2_Prompt_RefGT_week28_2017-v1/RECO'
print(dbs3api.listFileSummaries(dataset=dataset))
print("\n")

dataset = '/DoubleEG/CMSSW_9_2_3_patch2-2017_07_11_19_22_PRref_92X_dataRun2_Prompt_RefGT_week28_2017-v1/RECO'
print(dbs3api.listFileSummaries(dataset=dataset, sumOverLumi=1))
print("\n")

dataset = '/DoubleEG/CMSSW_9_2_3_patch2-2017_07_11_19_22_PRref_92X_dataRun2_Prompt_RefGT_week28_2017-v1/RECO'
print(dbs3api.listFileSummaries(dataset=dataset, run_num=297723))
print("\n")

dataset = '/DoubleEG/CMSSW_9_2_3_patch2-2017_07_11_19_22_PRref_92X_dataRun2_Prompt_RefGT_week28_2017-v1/RECO'
print(dbs3api.listFileSummaries(dataset=dataset, sumOverLumi=1, run_num=297723))
print("\n")


print("All Done")
