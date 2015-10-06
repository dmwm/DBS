import os
import logging
from functools import wraps

from WMCore.Configuration import Configuration, loadConfigurationFile
from WMCore.Database.DBFactory import DBFactory

def get_db_config(configfile, service='DBSReader', dbinstance='dev/global'):
    cfg = loadConfigurationFile(configfile)
    wconfig = cfg.section_("Webtools")
    app = wconfig.application
    
    appconfig = cfg.section_(app)
    dbsconfig = getattr(appconfig.views.active, service)
    dbconfig = getattr(dbsconfig.database.instances, dbinstance)

    return dbconfig.dbowner, dbconfig.connectUrl

class DaoConfig(object):
    def __init__(self, service):
        self.service = service
        
    def __call__(self, func,*args,**kwargs):
        @wraps(func)
        def wrapper(*args,**kwargs):
             config = os.environ['DBS_TEST_CONFIG']
             dbowner, connectUrl = get_db_config(configfile=config, service=self.service)

             test_class_instance = args[0]
             
             test_class_instance.logger = logging.getLogger("dbs test logger")
             test_class_instance.dbowner = dbowner
             test_class_instance.dbi = DBFactory(test_class_instance.logger, connectUrl).connect()

             out = func(*args, **kwargs)
             
             return out
        return wrapper
    
def add_dao_configuration(f):
    @wraps(f)
    def wrapper(self,*args,**kwargs):
        config = os.environ['DBS_TEST_CONFIG']
        service = os.environ.get("DBS_TEST_SERVICE", "DBSReader")
        
        dbowner, connectUrl = get_db_config(configfile=config, service=service)

        self.logger = logging.getLogger("dbs test logger")
        self.dbowner = dbowner
        self.dbi = DBFactory(self.logger, connectUrl).connect()
        f(self, *args, **kwargs)
        return
    return wrapper
