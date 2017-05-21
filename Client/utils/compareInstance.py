
import sys
import pprint
from time import sleep
#DBS-3 imports
from dbs.apis.dbsClient import *
#url="http://cmssrv18.fnal.gov:8585/dbs3"
#url for tunneling through lxplus 
#url="https://localhost:1443/dbs/DBSWriter"
#url="https://cmsweb-testbed.cern.ch/dbs/prod/global/DBSWriter"
url2="https://dbs3-test1.cern.ch/dbs/int/global/DBSWriter"
url1="https://dbs3-test1.cern.ch/dbs/int/global/DBSReader"
migrateURL="https://dbs3-test1.cern.ch/int/global/DBSMigrate"

# API Object    
#proxy=os.environ.get('SOCKS5_PROXY')
dbs3api1 = DbsApi(url=url1)
dbs3api2 = DbsApi(url=url2)
migrateApi = DbsApi(url=migrateURL)
#datatier = {'data_tier_name' : 'GEN-SIM-DBSDEBUG2' }
datatier = {'data_tier_name' :'TESTSPLIT-YUYI2'}
try:
    import pdb
    pdb.set_trace()
    dbs3api2.insertDataTier(datatier)
    ins1 = dbs3api1.listDataTiers()
    ins2 = dbs3api2.listDataTiers()
    l1 = len(ins1)
    l2 = len(ins2)
    print ("l1 = ", l1)
    print ("l2 = ", l2) 
    if l1 == l2:
        print ("read and write have the same array length.")
        for i in ins1:
           l = -1
           for j in ins2:
                l += 1 
                if i['create_by'] == j['create_by'] and \
                    i['data_tier_id'] == j['data_tier_id'] and \
                    i['data_tier_name'] ==j['data_tier_name'] and \
                    i['creation_date'] == j['creation_date']:
                   break;
           if l == l2:
              print ("l = ", l)
              if i['create_by'] != ins2[l2]['create_by'] or \
                 i['data_tier_id'] != ins2[l2]['data_tier_id'] or \
                 i['data_tier_name'] != ins2[l2]['data_tier_name'] or \
                 i['creation_date'] != ins2[l2]['creation_date']:
                 print ("read and write not match.\n")
                 print (i['create_by'], i['data_tier_id'], i['data_tier_name'],  i['creation_date'])                
    else:
       print("read(%s) and write(%s) have different lenth."%(len(ins1), len(ins2)))
       sys.exit()
    print("All Done Reader and Writer")
except Exception as e:
    print(e)
    sys.exit()

inputBK = "/BTagMu/Run2016H-LogErrorMonitor-PromptReco-v3/USER#56fb2a3c-9fd8-11e6-9951-001e67abf228"
ds = dbs3api1.listBlocks(block_name=inputBK)
if ds:
    print("block " , inputBK , " already in DB.")
    sys.exit()
else: 
    data = {'migration_input': inputBK, 'migration_url': 'https://cmsweb.cern.ch/dbs/prod/global/DBSReader'}
    result = migrateApi.submitMigration(data)
    print(result)
    status = migrateApi.statusMigration(block_name=inputBK)
    print(status)
    while (status != 2):
        status = migrateApi.statusMigration(block_name=inputBK)
        sleep (1.0)
    ds1 = dbs3api1.listDatadatasets(block_name=inputBK)
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(ds1)   




