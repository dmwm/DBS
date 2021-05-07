
#DBS-3 imports
from dbs.apis.dbsClient import *
#url="https://cmsweb-testbed.cern.ch:8443/dbs/int/global/DBSReader/"
url="https://cmsweb.cern.ch/dbs/prod/global/DBSReader/"
#url="https://dbs3-test1.cern.ch/dbs/dev/global/DBSReader/"
# API Object    
dbs3api = DbsApi(url=url)
# All datasets, NOT implemented yet in server
#print dbs3api.listDatasets(dataset_access_type="PRODUCTION")
print("\n")

datasetlst = ['/StreamExpressCosmics/Tier0_Test_SUPERBUNNIES_vocms001-SiStripPCLHistos-Express-v5/ALCARECO', '/Commissioning/Tier0_Test_SUPERBUNNIES_vocms001-HcalCalIsoTrk-PromptReco-v5/ALCARECO']
#
#print(dbs3api.listDatasets(dataset='/unittest_web_primary_ds_name_52666/acq_era_52666-v2671/GEN-SIM-RAW*'))

print("\n")
print("data_tier_name=RAW, processed_ds_name=*Run2016*")
print(dbs3api.listDatasets(data_tier_name='RAW', processed_ds_name='*Run2016*'))

print("\n")
print("dataset='/*/*Run2016*/RAW'")
print(dbs3api.listDatasets(dataset='/*/*Run2016*/RAW'))

print("\n")
print("data_tier_name=RAW, processed_ds_name=*Run2016*")
print(dbs3api.listDatasets(detail=1, data_tier_name='RAW', processed_ds_name='*Run2016*'))

print("\n")
print("dataset='/*/*Run2016*/RAW'")
print(dbs3api.listDatasets(detail=1, dataset='/*/*Run2016*/RAW'))

#print dbs3api.listDatasetArray(detail=0,dataset=datasetlst)
print("\n")
#print dbs3api.listDatasetArray(detail=1,dataset=datasetlst)
print("\n")
#print(dbs3api.listDatasetArray(detail=0, dataset='/unittest_web_primary_ds_name_34661/Acq_Era_34661-unittest_web_dataset-v4664/GEN-SIM-RAW'))
print("\n")
#print(dbs3api.listDatasetArray(detail=1, dataset='/unittest_web_primary_ds_name_34661/Acq_Era_34661-unittest_web_dataset-v4664/GEN-SIM-RAW'))
print("\n")
#print(dbs3api.listDatasets(detail=1))
print("\n")
#print(dbs3api.listDatasets(detail=1, dataset_access_type='*'))
print("\n")
#print dbs3api.listDatasetArray(dataset_id=9542504)
print("All Done")
