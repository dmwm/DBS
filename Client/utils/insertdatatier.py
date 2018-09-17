from __future__ import print_function
#DBS-3 imports
from dbs.apis.dbsClient import *
#url="http://cmssrv18.fnal.gov:8585/dbs3"
#url for tunneling through lxplus 
#url="https://localhost:1443/dbs/DBSWriter"
#url="https://cmsweb-testbed.cern.ch:8443/dbs/prod/global/DBSWriter"
url2="https://dbs3-test1.cern.ch/dbs/dev/global/DBSWriter"
url1="https://dbs3-test1.cern.ch/dbs/dev/global/DBSReader"
# API Object    
#proxy=os.environ.get('SOCKS5_PROXY')
dbs3api1 = DbsApi(url=url1)
dbs3api2 = DbsApi(url=url2)
#datatier = {'data_tier_name' : 'GEN-SIM-DBSDEBUG2' }
datatier = {'data_tier_name' :'TESTSPLIT-YUYI1'}
try:
    #import pdb
    #pdb.set_trace()
    print( dbs3api1.listDataTiers())
    print(dbs3api2.listDataTiers())
    dbs3api2.insertDataTier(datatier)
    print(dbs3api1.listDataTiers())
    print(dbs3api2.listDataTiers())
    #dbs3api.insertPrimaryDataset({"primary_ds_type":"mc" , "primary_ds_name":"RelVal151SingleGammaPt35"})
    print("All Done")
except Exception as e:
    print(e)

