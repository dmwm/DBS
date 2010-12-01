"""Used to parse the config file and get reader and writer instances informations from it"""

import sys
from WMCore.Configuration import loadConfigurationFile

def echoInfo(configfile):
    cfg = loadConfigurationFile(configfile)
    wconfig = cfg.CoreDatabase
    if sys.argv[2] == "database":
        print wconfig.connectUrl
    elif sys.argv[2] == "dbowner":
        print wconfig.dbowner
    else:
        print "Unknown config option: %s" % sys.argv[2]

if __name__ == "__main__":
    echoInfo(sys.argv[1])
