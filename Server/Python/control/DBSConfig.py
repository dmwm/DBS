"""
DBS Server  configuration file
"""
import os,logging,sys
from WMCore.Configuration import Configuration
from WMCore.WMInit import getWMBASE

#ROOTDIR = os.path.normcase(os.path.abspath(__file__)).rsplit('/', 3)[0]
#DBSVERSION = os.getenv('DBS3_VERSION')
ROOTDIR = os.getenv('DBS3_ROOT')

sys.path.append(os.path.join(ROOTDIR,'DBS/Server/Python/control'))

from DBSSecrets import connectUrl
from DBSSecrets import databaseOwner

config = Configuration()
config.component_('SecurityModule')
config.SecurityModule.key_file = os.path.join(ROOTDIR,'apache2/binkey')

config.component_('Webtools')
config.Webtools.port = 8787
config.Webtools.access_log_file = os.environ['DBS3_ROOT'] +"/Logs/single/cms_dbs_writer_access.log"
config.Webtools.error_log_file = os.environ['DBS3_ROOT'] +"/Logs/single/cms_dbs_writer_error.log"
config.Webtools.log_screen = True
config.Webtools.environment = "develop"
config.Webtools.autoreload = True
config.Webtools.show_tracebacks = True
config.Webtools.proxy_base = 'True'
config.Webtools.application = 'dbs'
config.Webtools.environment = 'development'
config.Webtools.error_log_level = logging.DEBUG

config.component_('dbs')
config.dbs.templates = os.path.join(getWMBASE(),'../../templates/WMCore/WebTools')
config.dbs.title = 'DBS Server'
config.dbs.description = 'CMS DBS Service'
config.dbs.section_('views')
config.dbs.admin = 'cmsdbs'
config.dbs.default_expires = 300
config.dbs.instances = ['prod/global','dev/global','int/global']

active = config.dbs.views.section_('active')
active.section_('DBSReader')
active.DBSReader.object = 'WMCore.WebTools.RESTApi'
active.DBSReader.section_('model')
active.DBSReader.model.object = 'dbs.web.DBSReaderModel'
active.DBSReader.section_('formatter')
active.DBSReader.formatter.object = 'WMCore.WebTools.RESTFormatter'
active.DBSReader.section_('database')
instances = active.DBSReader.database.section_('instances')

ProductionGlobal = instances.section_('prod/global')
ProductionGlobal.dbowner = databaseOwner['ProductionGlobal']
#ProductionGlobal.version = DBSVERSION
ProductionGlobal.connectUrl = connectUrl['ProductionGlobal']
ProductionGlobal.engineParameters = { 'pool_size': 15, 'max_overflow': 10, 'pool_timeout' : 200 }

DevelopmentGlobal = instances.section_('dev/global')
DevelopmentGlobal.dbowner = databaseOwner['DevelopmentGlobal']
#DevelopmentGlobal.version = DBSVERSION
DevelopmentGlobal.connectUrl = connectUrl['DevelopmentGlobal']
DevelopmentGlobal.engineParameters = { 'pool_size': 15, 'max_overflow': 10, 'pool_timeout' : 200 }

IntegrationGlobal = instances.section_('int/global')
IntegrationGlobal.dbowner = databaseOwner['IntegrationGlobal']
#IntegrationGlobal.version = DBSVERSION
IntegrationGlobal.connectUrl = connectUrl['IntegrationGlobal']
IntegrationGlobal.engineParameters = { 'pool_size': 15, 'max_overflow': 10, 'pool_timeout' : 200 }

active.section_('DBSWriter')
active.DBSWriter.object = 'WMCore.WebTools.RESTApi'
active.DBSWriter.section_('model')
active.DBSWriter.model.object = 'dbs.web.DBSWriterModel'
active.DBSWriter.section_('formatter')
active.DBSWriter.formatter.object = 'WMCore.WebTools.RESTFormatter'
active.DBSWriter.section_('database')
instances = active.DBSWriter.database.section_('instances')

ProductionGlobal = instances.section_('prod/global')
ProductionGlobal.dbowner = databaseOwner['ProductionGlobal']
#ProductionGlobal.version = DBSVERSION
ProductionGlobal.connectUrl = connectUrl['ProductionGlobal']
ProductionGlobal.engineParameters = { 'pool_size': 15, 'max_overflow': 10, 'pool_timeout' : 200 }

DevelopmentGlobal = instances.section_('dev/global')
DevelopmentGlobal.dbowner = databaseOwner['DevelopmentGlobal']
#DevelopmentGlobal.version = DBSVERSION
DevelopmentGlobal.connectUrl = connectUrl['DevelopmentGlobal']
DevelopmentGlobal.engineParameters = { 'pool_size': 15, 'max_overflow': 10, 'pool_timeout' : 200 }

IntegrationGlobal = instances.section_('int/global')
IntegrationGlobal.dbowner = databaseOwner['IntegrationGlobal']
#IntegrationGlobal.version = DBSVERSION
IntegrationGlobal.connectUrl = connectUrl['IntegrationGlobal']
IntegrationGlobal.engineParameters = { 'pool_size': 15, 'max_overflow': 10, 'pool_timeout' : 200 }
