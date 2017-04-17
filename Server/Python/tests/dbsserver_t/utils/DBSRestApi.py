"""
This module provides a stand-alone client for DBS server
Also DBSRestApi will be used in various stand-alone tests
"""
from __future__ import print_function
import json
import os, logging
import getpass
from cherrypy import request, log
from optparse import OptionParser

from WMCore.Configuration import Configuration, loadConfigurationFile
from WMCore.WebTools.RESTApi import RESTApi

class FileLike(object):
    """FileLike Object class with two methods:
    read - used for insert call
    write - as null device"""
    def __init__(self, data={}):
        self.data = json.dumps(data)
        
    def read(self):
        return self.data
    
    def write(self, s):
        pass

class DBSRestApi:
    def __init__(self, configfile, service='DBS', dbinstance='dev/global', migration_test=False):
        """migration_test is used to point the DBSRestApi to one specific database, no matter,
        to which DB the original config file points to."""
        log.error_log.setLevel(logging.ERROR)
        self.migration_test = migration_test
        config = self.configure(configfile, service, dbinstance)
        config = config.section_("DBS")
        config.instance = dbinstance
        self.rest = RESTApi(config)
        self.config = config

    def configure(self, configfile, service, dbinstance):
        cfg = loadConfigurationFile(configfile)
        wconfig = cfg.section_("Webtools")
        app = wconfig.application
        
        appconfig = cfg.section_(app)
        dbsconfig = getattr(appconfig.views.active, service)

        # Either we change formatter
        # OR change the 'Accept' type to application/json (which we don't know how to do at the moment)
        dbsconfig.formatter.object="WMCore.WebTools.RESTFormatter"
        config = Configuration()
         
        config.component_('SecurityModule')
        config.SecurityModule.dangerously_insecure = True

        config.component_('DBS')
        config.DBS.application = app
        config.DBS.model       = dbsconfig.model
        config.DBS.formatter   = dbsconfig.formatter

        #Does not support instances
        #config.DBS.instances   = cfg.dbs.instances
        #config.DBS.database    = dbsconfig.database

        if self.migration_test:
            #Use one specific database cms_dbs3_dev_phys02@int2r for migration unittests
            from DBSSecrets import dbs3_l3_i2
            config.DBS.section_('database')
            config.DBS.database.connectUrl = dbs3_l3_i2['connectUrl']['writer']
            config.DBS.database.dbowner = dbs3_l3_i2['databaseOwner']
            config.DBS.database.engineParameters = { 'pool_size' : 15, 'max_overflow' : 10, 'pool_timeout' : 200 }
            version = getattr(dbsconfig.database.instances, dbinstance).version
            config.DBS.database.version = version if version else '3.99.98'

            config.DBS.section_('security')
            config.DBS.security.params = {}

        else:
            #Use dev/global from dbs configuration for the reader, writer and dao unittests
            dbconfig = getattr(dbsconfig.database.instances, dbinstance)
            config.DBS.section_('database')
            config.DBS.database.connectUrl = dbconfig.connectUrl
            config.DBS.database.dbowner = dbconfig.dbowner
            config.DBS.database.engineParameters = dbconfig.engineParameters
            config.DBS.database.version = dbconfig.version if dbconfig.version else '3.99.98'
            #config.DBS.database.instance = dbconfig.instance

            try:
                secconfig = getattr(dbsconfig.security.instances, dbinstance)
            except AttributeError:
                pass
            else:
                config.DBS.section_('security')
                config.DBS.security.params = secconfig.params

        config.DBS.default_expires = 900

        return config

    def list1(self, call, params={}):
        """takes parameters as a dictionary"""
        request.method = 'GET'
        request.user = {'name' : getpass.getuser()}
        return self.rest.default(*[call], **params)
    
    def list(self, *args, **kwargs):
        """
        takes individual parameters
        Example: api.list('files',dataset='/a/b/c')
        """
        #print "List API call ....."
        request.method = 'GET'
        request.user = {'name' : getpass.getuser()}
        return self.parseForException(self.rest.default(*args, **kwargs))

    def insert(self, call, params={}):
        request.method = 'POST'
        request.body = FileLike(params)
        request.user = {'name' : getpass.getuser()}
        #Forcing NO use of insert buffer during the unit tests
        if call=='files':
            ret=self.rest.default(*[call, False])
        else:
            ret=self.rest.default(*[call])

        return self.parseForException(ret)

    def update(self, *args, **kwargs):
        request.method = 'PUT'
        request.user = {'name' : getpass.getuser()}
        ret=self.rest.default(*args, **kwargs)
        return self.parseForException(ret)

    def parseForException(self, data):
        if type(data)==type("abc"):
            data=json.loads(data)
        if type(data) == type({}) and 'exception' in data:
            raise Exception("DBS Server raised an exception: HTTPError %s :" %data['exception'] + (data['message']))
        return data

def options():
    defaultcfg = os.environ["DBS_TEST_CONFIG"]
    defaultservice = os.environ["DBS_TEST_SERVICE"]
    parser = OptionParser()
    parser.add_option("-c", "--config", dest="cfile", default=defaultcfg)
    parser.add_option("--service", dest="service", default=defaultservice)
    parser.add_option("--primary_ds_name", dest='primary_ds_name')
    parser.add_option("--processed_ds_name", dest='processed_ds_name')
    parser.add_option("--data_tier_name", dest='data_tier_name')
    parser.add_option("--dataset", dest='dataset')
    parser.add_option("--block_name", dest='block_name')
    parser.add_option("--logical_file_name", dest='logical_file_name')
    parser.add_option("--site_name", dest='site_name')
    parser.add_option("--parent_dataset", dest='parent_dataset')
    #parser.add_option("--version", dest='version')
    parser.add_option("--hash", dest='hash')
    parser.add_option("--app_name", dest='app_name')
    parser.add_option("--output_module_label", dest='output_module_label')
    parser.add_option("--run_num", dest='run_num')
    parser.add_option("--maxrun", dest='maxrun')
    parser.add_option("--minrun", dest='minrun')
    parser.add_option("--lumi_list", dest='lumi_list')
    parser.add_option("--origin_site_name", dest='origin_site_name')
    parser.add_option("--detail", dest='detail')
    opts, args = parser.parse_args()
    assert len(args) == 1
    allopts = opts.__dict__
    optdict = {}
    for key in allopts:
        if allopts[key]:
            optdict[key] = allopts[key]
    return args[0], optdict
        
    
if __name__ == "__main__":
    call, params = options()
    api = DBSRestApi(params["cfile"], params["service"])
    del params["cfile"]
    del params["service"]
    res = api.list1(call, params)
    dres = json.loads(res)
    pres = json.dumps(dres, sort_keys = True, indent = 4)
    print() 
    print(pres)
