#DBS-3 imports
from dbs.apis.dbsClient import *
#url="http://cmssrv48.fnal.gov:8989/DBSServlet"
url="http://cmssrv18.fnal.gov:8585/dbs3"
# API Object    
dbs3api = DbsApi(url=url)
block={'BLOCK_NAME': u'/TkCosmics38T/Summer09-STARTUP31X_V3-v1/GEN-SIM-DIGI-RAW#fc31bf9d-d44e-45a6-b87b-8fe6e2701062', 'FILE_COUNT': '500', 'ORIGIN_SITE': 'cmssrm.fnal.gov', 'LAST_MODIFICATION_DATE': '1255099739', 'CREATE_BY': 'cmsprod@caraway.hep.wisc.edu', 'BLOCK_SIZE': '1436055868219', 'OPEN_FOR_WRITING': 1, 'LAST_MODIFIED_BY': '/DC=org/DC=doegrids/OU=People/CN=Ajit Kumar Mohapatra 867118', 'CREATION_DATE': '1255020532'}
print dbs3api.insertBlock(block)
