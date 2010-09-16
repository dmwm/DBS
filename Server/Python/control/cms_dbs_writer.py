"""
DBS Server  configuration file
"""
import os, logging
from WMCore.Configuration import Configuration
import WMCore.WMInit

#Oracle
#databaseOwner='CMS_LUM_OWNER'

#Mysql
databaseOwner='__MYSQL__'

DBSVersion = 'DBS_3_0_0'

config = Configuration()

#Agent section is required by WMCore
config.section_("Agent")
config.Agent.contact ="dbs_support@cern.ch"
#Only these two lines in "Agent" are required in order to turn off the default services.
config.Agent.useMsgService = False
config.Agent.useTrigger = False

config.section_("General")
config.General.workDir = os.environ['DBS3_ROOT']

#CoreDatabase section is required by WMCore. 
config.section_("CoreDatabase")
# User specific parameter
config.CoreDatabase.version = DBSVersion
config.CoreDatabase.dbowner = databaseOwner
#mySql
config.CoreDatabase.connectUrl = 'mysql://user:pd@cmssrv18.fnal.gov:3307/CMS_DBS3'
config.CoreDatabase.engineParameters = {'pool_size': 50, 'max_overflow': 10, 'pool_timeout':200 }
#Oracle
#config.CoreDatabase.connectUrl = 'oracle://user:pd@cmscald'
#config.CoreDatabase.engineParameters = {'pool_size': 15, 'max_overflow': 10, 'pool_timeout': 200 }

#config web server. These are required fields by WMCore althrough some of them are useless.
config.webapp_("cmsdbs")
config.cmsdbs.componentDir = config.General.workDir + "/Logs/DBSServer"
config.cmsdbs.server.host = "::"
config.cmsdbs.server.port = 8688
config.cmsdbs.templates = WMCore.WMInit.getWMBASE() + '/src/templates/WMCore/WebTools'
config.cmsdbs.admin = "yuyi@fnal.gov"
config.cmsdbs.title = 'DBS Server'
config.cmsdbs.dbowner = databaseOwner
config.cmsdbs.description = 'CMS DBS Service'
config.cmsdbs.default_expires=300
config.cmsdbs.section_('views')
active=config.cmsdbs.views.section_('active')

#DBS server page/view
DBS = active.section_('DBS')
DBS.object = 'WMCore.WebTools.RESTApi'
DBS.section_('model')
DBS.model.object = 'dbs.web.DBSWriterModel'
DBS.section_('formatter')
active.DBS.formatter.object = 'WMCore.WebTools.RESTFormatter'

#Migration server page/view
MIGRATE = active.section_('MIGRATE')
MIGRATE.object = 'WMCore.WebTools.RESTApi'
MIGRATE.section_('model')
MIGRATE.model.object = 'dbs.web.DBSMigrateModel'
MIGRATE.section_('formatter')
MIGRATE.formatter.object = 'WMCore.WebTools.RESTFormatter'
MIGRATE.version = DBSVersion
MIGRATE.nthreads = 4

#config migration mover
config.component_('DBSMigrationMover')
config.DBSMigrationMover.default_expires=300
config.DBSMigrationMover.pollInterval = 1
config.DBSMigrationMover.namespace= "dbs.components.migration.DBSMigrationMover"
config.DBSMigrationMover.componentDir = config.General.workDir + "/Logs/MigrationMover"
config.DBSMigrationMover.workerThreads = 1

#Config insert buffer
config.component_('DBSInsertBuffer')
config.DBSInsertBuffer.default_expires=300
config.DBSInsertBuffer.pollInterval = 1
config.DBSInsertBuffer.namespace= "dbs.components.insertbuffer.DBSInsertBuffer"
config.DBSInsertBuffer.componentDir = config.General.workDir + "Logs/DBSInsertBuffer"
config.DBSInsertBuffer.workerThreads = 1
