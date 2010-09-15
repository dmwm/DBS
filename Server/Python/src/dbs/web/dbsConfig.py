"""
DBS Server configuration file
"""

from WMCore.Configuration import Configuration
import os

config = Configuration()

# This component has all the configuration of CherryPy
config.component_('Webtools')
config.Webtools.host = 'cmssrv18.fnal.gov' 
config.Webtools.application = 'DBSServer'
# This is the config for the application
config.component_('DBSServer')
# Define the default location for templates for the app
config.DBSServer.templates = os.environ['WTBASE']
config.DBSServer.title = 'DBS Server'
config.DBSServer.description = 'CMS DBS Service'
config.DBSServer.admin = 'anzar@fnal.gov'

# Views are all pages 
config.DBSServer.section_('views')

# These are all the active pages that Root.py should instantiate 
active=config.DBSServer.views.section_('active')
active.section_('dbs')
active.dbs.object = 'WMCore.WebTools.RESTApi'
###oracle://username:password@tnsname
active.dbs.database = 'oracle://CMS_DBS3_OWNER:new4_dbs3@cmscald'
active.dbs.section_('model')
active.dbs.model.object = 'dbs.web.dbsModel'
active.dbs.section_('formatter')
active.dbs.formatter.object = 'WMCore.WebTools.RESTFormatter'

#active.rest.logLevel = 'DEBUG'
# DASCacheMgr settings:
# sleep defines interval of checking cache queue
# verbose defines level of logger
#    active.rest.sleep = 1
#    active.rest.verbose = 0


