"""
DBS Server  configuration file
"""
import os, logging
from WMCore.Configuration import Configuration

config = Configuration()
config.component_('SecurityModule')
config.SecurityModule.dangerously_insecure = True


config.component_('Webtools')
config.Webtools.port = 8787
config.Webtools.host = '::'
config.Webtools.access_log_file = os.environ['DBS3_ROOT'] +"/Logs/single/cms_dbs_writer_access.log"
config.Webtools.error_log_file = os.environ['DBS3_ROOT'] +"/Logs/single/cms_dbs_writer_error.log"
#for debugging. Everything goes to screen
config.Webtools.log_screen = True
"""
#below three settting is for code debugging.
config.Webtools.environment = "develop"
config.Webtools.autoreload = True
config.Webtools.show_tracebacks = True
"""
config.Webtools.application = 'cms_dbs'

config.component_('cms_dbs')
config.cms_dbs.templates = os.environ['WTBASE'] + '/templates/WMCore/WebTools'
config.cms_dbs.title = 'DBS Server'
config.cms_dbs.description = 'CMS DBS Service'
config.cms_dbs.section_('views')
config.cms_dbs.admin='cmsdbs'
config.cms_dbs.default_expires=300 

active=config.cms_dbs.views.section_('active')
active.section_('DBS')
active.DBS.object = 'WMCore.WebTools.RESTApi'
active.DBS.section_('model')
active.DBS.model.object = 'dbs.web.DBSWriterModel'
active.DBS.section_('formatter')
active.DBS.formatter.object = 'WMCore.WebTools.RESTFormatter'

#Oracle
active.DBS.dbowner = 'OWNER'
active.DBS.version = 'DBS_3_0_0'
active.DBS.section_('database')
active.DBS.database.connectUrl = 'oracle://username:pd@dbname'
active.DBS.database.engineParameters = {'pool_size': 15, 'max_overflow': 10, 'pool_timeout' : 200 }

#mysql
"""
active.DBS.dbowner = '__MYSQL__'
active.DBS.version = 'DBS_3_0_0'
active.DBS.section_('database')
active.DBS.database.connectUrl = 'mysql://username:pd@servername.fnal.gov:port/DATABASE_NAME'
active.DBS.database.engineParameters = {'pool_size': 50, 'max_overflow': 10, 'pool_timeout' : 200 }
"""

active.section_('MIGRATE')
active.MIGRATE.object = 'WMCore.WebTools.RESTApi'
active.MIGRATE.section_('model')
active.MIGRATE.model.object = 'dbs.web.DBSMigrateModel'
active.MIGRATE.section_('formatter')
active.MIGRATE.formatter.object = 'WMCore.WebTools.RESTFormatter'
active.MIGRATE.section_('database')
#active.MIGRATE.database.connectUrl = 'mysql://username:pd@servername.fnal.gov:port/database_name'
#active.MIGRATE.dbowner = '__MYSQL__'
active.MIGRATE.database.connectUrl = 'oracle://username:pd@dbname'
active.MIGRATE.dbowner = 'OWNER'
active.MIGRATE.version = 'DBS_3_0_0'
active.MIGRATE.nthreads = 4
