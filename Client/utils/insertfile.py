#DBS-3 imports
from dbs.apis.dbsClient import *
#url="http://cmssrv48.fnal.gov:8989/DBSServlet"
url="http://cmssrv18.fnal.gov:8585/dbs3"
# API Object    
dbs3api = DbsApi(url=url)
file={'FILE_TYPE': 'EDM', 'LOGICAL_FILE_NAME': '/store/mc/Summer09/TkCosmics38T/GEN-SIM-DIGI-RAW/STARTUP31X_V3-v1/0010/66EE7132-FFB3-DE11-9D33-001E682F1FA6.root', 'FILE_SIZE': '2824329131', 'LAST_MODIFICATION_DATE': '1255099729', 'FILE_PARENT_LIST': [], 'AUTO_CROSS_SECTION': 0.0, 'MD5': 'NOTSET', 'CHECK_SUM': '862355611', 'FILE_LUMI_LIST': [{'LUMI_SECTION_NUM': u'10018', 'RUN_NUM': '1'}], 'ADLER32': 'NOTSET', 'EVENT_COUNT': '2041', 'CREATE_BY': 'cmsprod@caraway.hep.wisc.edu', 'LAST_MODIFIED_BY': '/DC=org/DC=doegrids/OU=People/CN=Ajit Kumar Mohapatra 867118', 'DATASET': '/TkCosmics38T/Summer09-STARTUP31X_V3-v1/GEN-SIM-DIGI-RAW', 'BLOCK': '/TkCosmics38T/Summer09-STARTUP31X_V3-v1/GEN-SIM-DIGI-RAW#fc31bf9d-d44e-45a6-b87b-8fe6e2701062', 'IS_FILE_VALID': 1}

print dbs3api.insertFile([file])

