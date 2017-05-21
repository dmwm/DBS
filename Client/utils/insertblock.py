
#DBS-3 imports
from dbs.apis.dbsClient import *
url="http://cmssrv18.fnal.gov:8585/dbs3"
# API Object    
dbs3api = DbsApi(url=url)
block={'site_list' : ['cmsdcache.pi.infn.it'], 'block_name': '/anzar22/Summer09-STARTUP31X_V3-v1/GEN-SIM-DIGI-RAW#1237f9d-d44e-45a6-b87b-8fe6e27011162', 'file_count': '500', 'origin_site': 'cmssrm.fnal.gov', 'last_modification_date': '1255099739', 'create_by': 'cmsprod@caraway.hep.wisc.edu', 'block_size': '1436055868219', 'open_for_writing': 1, 'last_modified_by': '/DC=org/DC=doegrids/OU=People/CN=Ajit Kumar Mohapatra 867118', 'creation_date': '1255020532'}
print(list(block.keys()))
try:
    dbs3api.insertBlock(block)
except Exception as e:
    print(e)

