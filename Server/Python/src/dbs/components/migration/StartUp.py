#!/usr/bin/env python
from optparse import OptionParser
import cherrypy, logging, sys

from WMCore.Configuration import Configuration, loadConfigurationFile
from dbs.components.migration.DBSMigrationServer import DBSMigrationServer, MigrationTask, MigrationWebMonitoring

def get_command_line_options(executable_name, arguments):
    parser = OptionParser(usage='%s options' % executable_name)
    parser.add_option('-c', '--config', type='string', dest='config', help='Migration Server Configuration')

    options, args = parser.parse_args()
    if not (options.config):
        parser.print_help()
        parser.error('You need to provide following options, --config=DefaultConfig.py')
    return options

def configure(configfile):
    cfg = loadConfigurationFile(configfile)
    web_cfg = cfg.web.dictionary_()

    ###configure cherry py
    cherrypy_cfg = {'server.host' : web_cfg.get('host', '127.0.0.1'),
                    'server.socket_port' : web_cfg.get('port', 8251),
                    'log.screen' : web_cfg.get('log_screen', False),
                    'server.thread_pool' : web_cfg.get('thread_pool', 10)}

    cherrypy.config.update(cherrypy_cfg)

    error_log_level = web_cfg.get('error_log_level', logging.WARNING)
    access_log_level = web_cfg.get("access_log_level", logging.INFO)
    cherrypy.log.error_log.setLevel(error_log_level)
    cherrypy.log.access_log.setLevel(access_log_level)

    migration_cfg = cfg.dbsmigration

    migration_config = {}

    for instance in migration_cfg.instances:
        instance_settings = getattr(migration_cfg.database.instances, instance)
        migration_config.setdefault('database',{}).update({instance :
                                                           {'threads' : instance_settings.threads,
                                                            'dbowner' : instance_settings.dbowner,
                                                            'engineParameters' : instance_settings.engineParameters,
                                                            'connectUrl' : instance_settings.connectUrl}})

    return migration_config

if __name__ == '__main__':
    options = get_command_line_options(__name__, sys.argv)
    migration_config = configure(options.config)

    for instance in migration_config['database'].keys():
        for thread in xrange(migration_config['database'][instance]['threads']):
            DBSMigrationServer(MigrationTask(migration_config['database'][instance]), duration = 5)

    root = MigrationWebMonitoring()

    cherrypy.log.error_log.info("*********** DBS Migration Server Starting. ************")
    ##mount tree and start service
    cherrypy.tree.mount(root)
    cherrypy.quickstart(root)
