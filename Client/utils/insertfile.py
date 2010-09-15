#DBS-3 imports
from dbs.apis.dbsClient import *
#url="http://cmssrv48.fnal.gov:8989/DBSServlet"
url="http://cmssrv18.fnal.gov:8585/dbs3"
# API Object    
dbs3api = DbsApi(url=url)
file={'FILE_TYPE': 'EDM', 'LOGICAL_FILE_NAME': u'/store/mc/Summer09/TkCosmics38T/GEN-SIM-DIGI-RAW/STARTUP31X_V3-v1/0010/66EE7132-FFB3-DE11-9D33-001E682F1FA6.root', 'FILE_SIZE': u'2824329131', 'LAST_MODIFICATION_DATE': u'1255099729', 'FILE_PARENT_LIST': [], 'AUTO_CROSS_SECTION': 0.0, 'MD5': u'NOTSET', 'CHECK_SUM': u'862355611', 'FILE_LUMI_LIST': [{'LUMI_SECTION_NUM': u'10018', 'RUN_NUM': u'1'}], 'ADLER32': u'NOTSET', 'EVENT_COUNT': u'2041', 'CREATE_BY': u'cmsprod@caraway.hep.wisc.edu', 'LAST_MODIFIED_BY': u'/DC=org/DC=doegrids/OU=People/CN=Ajit Kumar Mohapatra 867118', 'DATASET': u'/TkCosmics38T/Summer09-STARTUP31X_V3-v1/GEN-SIM-DIGI-RAW', 'BLOCK': u'/TkCosmics38T/Summer09-STARTUP31X_V3-v1/GEN-SIM-DIGI-RAW#fc31bf9d-d44e-45a6-b87b-8fe6e2701062', 'IS_FILE_VALID': 1}

print dbs3api.insertFile([file])

