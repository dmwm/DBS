"""
DBS Server  configuration file
"""

__revision__ = "$Id: DBSConfig.py,v 1.6 2009/11/19 18:58:10 akhukhun Exp $"
__version__ = "$Revision: 1.6 $"


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
#active.dbs3.database = 'mysql://dbs:cmsdbs@localhost:3306/DBS_3_0_0'
#active.dbs3.database = 'oracle://cms_dbs_afaq:anzpw03062009@devdb10'
#active.dbs3.database = 'oracle://cms_dbs_afaq:anzpw03062009@oradev10.cern.ch:10520/D10'
active.dbs3.database = 'oracle://CMS_DBS3_OWNER:new4_dbs3@uscmsdb03.fnal.gov:1521/cmscald'
active.dbs3.section_('model')
active.dbs3.model.object = 'dbs.web.DBSModel'
active.dbs3.section_('formatter')
active.dbs3.formatter.object = 'WMCore.WebTools.RESTFormatter'
