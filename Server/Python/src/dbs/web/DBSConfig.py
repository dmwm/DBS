"""
DBS Server  configuration file
"""

__revision__ = "$Id: DBSConfig.py,v 1.8 2009/11/27 09:07:18 akhukhun Exp $"
__version__ = "$Revision: 1.8 $"


import os, logging
from WMCore.Configuration import Configuration

config = Configuration()

config.component_('Webtools')
config.Webtools.port = 8585
config.Webtools.host = '::' 
config.Webtools.access_log_file = os.environ['DBS3_SERVER_ROOT'] +"/logs/access.log"
config.Webtools.error_log_file = os.environ['DBS3_SERVER_ROOT'] +"/logs/error.log"
config.Webtools.log_screen = False
config.Webtools.application = 'DBSServer'

config.component_('DBSServer')
config.DBSServer.templates = os.environ['WTBASE'] + '/templates/WMCore/WebTools'
config.DBSServer.title = 'DBS Server'
config.DBSServer.description = 'CMS DBS Service'

config.DBSServer.section_('views')

active=config.DBSServer.views.section_('active')
active.section_('dbs3')
active.dbs3.object = 'WMCore.WebTools.RESTApi'

active.dbs3.section_('model')
active.dbs3.model.object = 'dbs.web.DBSModel'
active.dbs3.section_('formatter')
active.dbs3.formatter.object = 'WMCore.WebTools.RESTFormatter'

active.dbs3.database = 'oracle://use_name:password@host:port/sid'
active.dbs3.dbowner = 'schema_owner'
