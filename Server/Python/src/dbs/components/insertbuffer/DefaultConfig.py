"""
DBS  insert buffer configuration file
"""

__revision__ = "$Id: DefaultConfig.py,v 1.3 2010/08/02 20:49:34 afaq Exp $"
__version__ = "$Revision: 1.3 $"

import os, logging
from WMCore.Configuration import Configuration

config = Configuration()

config.section_("Agent")
config.Agent.hostName = "cmssrv49.fnal.gov"
config.Agent.contact = "anzar@fnal.gov"
config.Agent.teamName = "DBS"
config.Agent.agentName = "DBSWriteBehindCache"
config.Agent.useMsgService = False
config.Agent.useTrigger = False
    
config.section_("General")
config.General.workDir = "/uscms/home/anzar/devDBS3/DBS3_ROOT"

config.section_("CoreDatabase")
config.CoreDatabase.connectUrl = 'oracle://CMS_LUM_WRITER:every1needsLumi!@cmscald'
config.CoreDatabase.dialect = "oracle"
config.CoreDatabase.dbowner = 'CMS_LUM_OWNER'

"""
config.section_("CoreDatabase")
config.CoreDatabase.connectUrl = "mysql://dbs3:dbs3_pass@cmssrv49.fnal.gov:3306/CMS_DBS3_ANZ_3"
config.CoreDatabase.dialect = "mysql"
config.CoreDatabase.dbowner = '__MYSQL__'
#config.CoreDatabase.socket = os.getenv("DBSOCK")
"""


config.component_('DBSInsertBuffer')
config.DBSInsertBuffer.default_expires=300
config.DBSInsertBuffer.pollInterval = 1 
config.DBSInsertBuffer.namespace= "dbs.components.insertbuffer.DBSInsertBuffer"
config.DBSInsertBuffer.componentDir = config.General.workDir + "/DBSInsertBuffer"
#config.DBSInsertBuffer.dbowner = '__MYSQL__'
config.DBSInsertBuffer.workerThreads = 1

