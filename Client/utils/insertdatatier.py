#DBS-3 imports
from dbs.apis.dbsClient import *
#url="http://cmssrv18.fnal.gov:8585/dbs3"
#url for tunneling through lxplus 
#url="https://localhost:1443/dbs/DBSWriter"
url="https://cmsweb-testbed.cern.ch/dbs/prod/global/DBSWriter"
#url="http://cms-xen39.fnal.gov:8787/dbs/int/global/DBSWriter"
# API Object    
#proxy=os.environ.get('SOCKS5_PROXY')
dbs3api = DbsApi(url=url)
#datatier = {'data_tier_name' : 'GEN-SIM-DBSDEBUG2' }
datatier = {'data_tier_name' :'GEN-SIM'}
try:
    #import pdb
    #pdb.set_trace()
    #print dbs3api.listDataTiers()
    dbs3api.insertDataTier(datatier)
    #dbs3api.insertPrimaryDataset({"primary_ds_type":"mc" , "primary_ds_name":"RelVal151SingleGammaPt35"})
    print "All Done"
except Exception, e:
    print e

