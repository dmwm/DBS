#DBS-3 imports
from dbs.apis.dbsClient import *
url="https://dbs3-test2.cern.ch/dbs/dev/global/DBSWriter"
# API Object    
dbs3api = DbsApi(url=url)
lfn=['/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/34661/1.root',
     '/store/mc/Fall08/BBJets250to500-madgraph/GEN-SIM-RAW/IDEAL_/34661/2.root'
    ]
dbs3api.updateFileStatus(logical_file_name=lfn, is_file_valid=1)

