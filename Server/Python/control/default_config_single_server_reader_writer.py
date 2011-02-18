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
config.Webtools.host = '0.0.0.0'
config.Webtools.access_log_file = os.environ['DBS3_ROOT'] +"/Logs/single/cms_dbs_writer_access.log"
config.Webtools.error_log_file = os.environ['DBS3_ROOT'] +"/Logs/single/cms_dbs_writer_error.log"
#for debugging. Everything goes to screen
config.Webtools.log_screen = False
config.Webtools.application = 'cms_dbs'

config.component_('cms_dbs')
config.cms_dbs.templates = os.environ['WTBASE'] + '/templates/WMCore/WebTools'
config.cms_dbs.title = 'DBS Server'
config.cms_dbs.description = 'CMS DBS Service'
config.cms_dbs.section_('views')
config.cms_dbs.admin='cmsdbs'
config.cms_dbs.default_expires=300 

active=config.cms_dbs.views.section_('active')
active.section_('DBSReader')
active.DBSReader.object = 'WMCore.WebTools.RESTApi'
active.DBSReader.section_('model')
active.DBSReader.model.object = 'dbs.web.DBSReaderModel'
active.DBSReader.section_('formatter')
active.DBSReader.formatter.object = 'WMCore.WebTools.RESTFormatter'

#Oracle
active.DBSReader.dbowner = 'OWNER'
active.DBSReader.version = 'DBS_3_0_0'
active.DBSReader.section_('database')
active.DBSReader.database.connectUrl = 'oracle://username:pd@dbname'
active.DBSReader.database.engineParameters = {'pool_size': 15, 'max_overflow': 10, 'pool_timeout' : 200 }

#mysql
"""
active.DBSReader.dbowner = '__MYSQL__'
active.DBSReader.version = 'DBS_3_0_0'
active.DBSReader.section_('database')
active.DBSReader.database.connectUrl = 'mysql://username:pd@servername.fnal.gov:port/DATABASE_NAME'
active.DBSReader.database.engineParameters = {'pool_size': 50, 'max_overflow': 10, 'pool_timeout' : 200 }
"""

