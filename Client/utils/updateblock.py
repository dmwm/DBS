#DBS-3 imports
from dbs.apis.dbsClient import *
#url="http://cmssrv18.fnal.gov:8585/dbs3"
#url="https://cmsweb.cern.ch:8443/dbs/prod/phys03/DBSWriter"
url="https://dbs3-testbed.cern.ch/dbs/dev/global/DBSWriter"
# API Object    
dbs3api = DbsApi(url=url)
#bk="/GlobalAug07-C/Online/RAW#ffe2395a-9c7c-434f-aad2-9b212f475df9"
#bk='/test_JHU_JJH0M/zghiche-test_JHU_JJH0M-d4a67aa6918b70024156b97001359548/USER#e5ccbcf4-37e5-4617-b81d-e0aad7bc5d94'
bk="/unittest_web_primary_ds_name_34661/Acq_Era_34661-unittest_web_dataset-v4664/GEN-SIM-RAW#34661"
dbs3api.updateBlockStatus(block_name=bk, open_for_writing=1)
dbs3api.updateBlockStatus(block_name=bk, open_for_writing=0)


