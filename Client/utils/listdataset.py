from __future__ import print_function
#DBS-3 imports
from dbs.apis.dbsClient import *
#url="https://cmsweb.cern.ch/dbs/prod/global/DBSReader/"
url="https://dbs3-test1.cern.ch/dbs/dev/global/DBSReader/"
# API Object    
dbs3api = DbsApi(url=url)
# All datasets, NOT implemented yet in server
#print dbs3api.listDatasets(dataset_access_type="PRODUCTION")
print("\n")

datasetlst = ['/StreamExpressCosmics/Tier0_Test_SUPERBUNNIES_vocms001-SiStripPCLHistos-Express-v5/ALCARECO', '/Commissioning/Tier0_Test_SUPERBUNNIES_vocms001-HcalCalIsoTrk-PromptReco-v5/ALCARECO']
#
#print dbs3api.listDatasetArray(detail=0,dataset=datasetlst)
print("\n")
#print dbs3api.listDatasetArray(detail=1,dataset=datasetlst)
print("\n")
print(dbs3api.listDatasetArray(detail=0, dataset='/unittest_web_primary_ds_name_34661/Acq_Era_34661-unittest_web_dataset-v4664/GEN-SIM-RAW'))
print("\n")
print(dbs3api.listDatasetArray(detail=1, dataset='/unittest_web_primary_ds_name_34661/Acq_Era_34661-unittest_web_dataset-v4664/GEN-SIM-RAW'))
print("\n")
print("\n")
#print dbs3api.listDatasetArray(dataset_id=9542504)
print("All Done")
