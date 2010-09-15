"""
DBS Server configuration file
"""

from WMCore.Configuration import Configuration

config = Configuration()

# This component has all the configuration of CherryPy
config.component_('Webtools')
#config.Webtools.host = '127.0.0.1' 
config.Webtools.application = 'DBSServer'
# This is the config for the application
config.component_('DBSServer')
# Define the default location for templates for the app
#config.DBSServer.templates = '/Users/ak427/Work/DBS3/WMCORE/src/python/WMCore/HTTPFrontEnd/REST/Templates/tmpl'
config.DBSServer.templates = '/Users/ak427/Work/DBS3/WMCORE/src/templates/WMCore/WebTools/'
config.DBSServer.title = 'DBS Server'
config.DBSServer.description = 'CMS DBS Service'
config.DBSServer.admin = 'ak427@cornell.edu'

# Views are all pages 
config.DBSServer.section_('views')

# These are all the active pages that Root.py should instantiate 
active=config.DBSServer.views.section_('active')
active.section_('dbs')
active.dbs.object = 'WMCore.WebTools.RESTApi'
#active.rest.templates = '/Users/ak427/Work/DBS3/WMCORE/src/templates/WMCore/WebTools/'
active.dbs.database = 'mysql://dbs:cmsdbs@localhost:3306/DBS_3_0_0'
active.dbs.section_('model')
active.dbs.model.object = 'dbs.web.dbsModel'
#active.rest.model.templates = '/Users/ak427/Work/DBS3/WMCORE/src/templates/WMCore/WebTools/'
active.dbs.section_('formatter')
active.dbs.formatter.object = 'WMCore.WebTools.RESTFormatter'
#active.rest.formatter.templates = '/Users/ak427/Work/DBS3/WMCORE/src/templates/WMCore/WebTools/'

#active.rest.logLevel = 'DEBUG'
# DASCacheMgr settings:
# sleep defines interval of checking cache queue
# verbose defines level of logger
#    active.rest.sleep = 1
#    active.rest.verbose = 0


