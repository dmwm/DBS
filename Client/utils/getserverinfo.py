#DBS-3 imports
from dbs.apis.dbsClient import *
#url="http://cmssrv48.fnal.gov:8989/DBSServlet"
url="http://cmssrv18.fnal.gov:8585/dbs3"
#url="http://vocms09.cern.ch:8989/DBSServlet"
#url="http://vocms09.cern.ch:8585/dbs3"
# API Object    
dbs3api = DbsApi(url=url)
print dbs3api.serverinfo()
