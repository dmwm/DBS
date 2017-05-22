from __future__ import print_function
#DBS-3 imports
import time
import pprint
from dbs.apis.dbsClient import *
url="https://dbs3-test1.cern.ch/dbs/dev/global/DBSReader/"
# API Object    
dbs3api = DbsApi(url=url)
pp = pprint.PrettyPrinter(indent=4)

#pp.pprint (dbs3api.listFiles(dataset="/RelValProdTTbar/CMSSW_3_1_0_pre11-MC_31X_V1-v1/GEN-SIM-RAW"))

print ('****************************************\n')
pp.pprint (dbs3api.listFiles(dataset="/unittest_web_primary_ds_name_34661/Acq_Era_34661-unittest_web_dataset-v4664/GEN-SIM-RAW", validFileOnly=1 ))

print ("****************************************\n")
pp.pprint (dbs3api.listFiles(dataset="/unittest_web_primary_ds_name_34661/Acq_Era_34661-unittest_web_dataset-v4664/GEN-SIM-RAW", validFileOnly=1, detail=1 ))

print ("****************************************\n")
pp.pprint (dbs3api.listFiles(dataset="/unittest_web_primary_ds_name_34661/Acq_Era_34661-unittest_web_dataset-v4664/GEN-SIM-RAW", validFileOnly=1, detail=1, sumOverLumi=1 ))

print ("****************************************\n")
pp.pprint (dbs3api.listFiles(logical_file_name='/store/data/Acq_Era_58715/unittest_web_primary_ds_name_58715/SIM/98/000000000/58715_2.root', validFileOnly=1, detail=1 ))

print ("****************************************\n")
pp.pprint (dbs3api.listFiles(logical_file_name='/store/data/Acq_Era_58715/unittest_web_primary_ds_name_58715/SIM/98/000000000/58715_2.root', validFileOnly=1, detail=1, sumOverLumi=1 ))

print ("****************************************\n")
pp.pprint (dbs3api.listFiles(logical_file_name='/store/data/Acq_Era_58715/unittest_web_primary_ds_name_58715/SIM/98/000000000/58715_2.root', validFileOnly=1, detail=1, run_num=55 ))

print ("****************************************\n")
pp.pprint (dbs3api.listFiles(logical_file_name='/store/data/Acq_Era_58715/unittest_web_primary_ds_name_58715/SIM/98/000000000/58715_2.root', validFileOnly=1, detail=1, run_num=[55, 76] ))

print ("****************************************\n")
pp.pprint (dbs3api.listFiles(logical_file_name='/store/data/Acq_Era_58715/unittest_web_primary_ds_name_58715/SIM/98/000000000/58715_2.root', validFileOnly=1, detail=1, run_num='55-76', sumOverLumi=1 ))

print ("****************************************\n")
pp.pprint (dbs3api.listFiles(logical_file_name='/store/data/Acq_Era_58715/unittest_web_primary_ds_name_58715/SIM/98/000000000/58715_2.root', validFileOnly=1, detail=1, run_num=[55,76], sumOverLumi=1 ))

print ("****************************************\n")
pp.pprint (dbs3api.listFileArray(logical_file_name='/store/data/Acq_Era_58715/unittest_web_primary_ds_name_58715/SIM/98/000000000/58715_2.root', validFileOnly=1, detail=1, run_num='55-76'))

print ("****************************************\n")
pp.pprint (dbs3api.listFileArray(logical_file_name='/store/data/Acq_Era_58715/unittest_web_primary_ds_name_58715/SIM/98/000000000/58715_2.root', validFileOnly=1, detail=1, run_num='55-76', sumOverLumi=1))

print ("****************************************\n")
pp.pprint (dbs3api.listFileArray(dataset="/unittest_web_primary_ds_name_34661/Acq_Era_34661-unittest_web_dataset-v4664/GEN-SIM-RAW", validFileOnly=1, detail=1, sumOverLumi=1 ))
