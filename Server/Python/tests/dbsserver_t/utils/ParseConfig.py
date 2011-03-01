"""Used to parse the config file and get reader and writer instances informations from it"""

import sys
from WMCore.Configuration import loadConfigurationFile

def echoInfo(configfile):
    cfg = loadConfigurationFile(configfile)
    #FIXME: Need to swith between different config files.
    """
    #This is used for config file of wmcoreD
    wconfig = cfg.CoreDatabase
    if sys.argv[2] == "database":
        print wconfig.connectUrl
    elif sys.argv[2] == "dbowner":
        print wconfig.dbowner
    else:
        print "Unknown config option: %s" % sys.argv[2]
    """
    #This is for root.py's configure file
    wconfig = cfg.section_("Webtools") 
    app = wconfig.application 
    appconfig = cfg.section_(app) 
    service = list(appconfig.views.active._internal_children)[0] 
    dbsconfig = getattr(appconfig.views.active, service)
    if sys.argv[2] == "database": 
        if 'database' in dbsconfig._internal_children: 
            print dbsconfig.database.connectUrl 
        else: 
            print dbsconfig.database 
    elif sys.argv[2] == "dbowner": 
        print dbsconfig.dbowner 
    else:
        print "Unknown config option: %s" % sys.argv[2] 
if __name__ == "__main__":
    echoInfo(sys.argv[1])
