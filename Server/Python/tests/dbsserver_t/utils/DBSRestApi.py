"""
This module provides a stand-alone client for DBS server
Also DBSRestApi will be used in various stand-alone tests
"""

__revision__ = "$Id: DBSRestApi.py,v 1.3 2010/01/14 16:53:24 afaq Exp $"
__version__ = "$Revision: 1.3 $"

import json
import os, logging
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
    def __init__(self, configfile):
        log.error_log.setLevel(logging.ERROR)
        config = self.configure(configfile)
        config = config.section_("DBS")
        self.rest = RESTApi(config)
        self.config = config

    def configure(self, configfile):
        cfg = loadConfigurationFile(configfile)
        wconfig = cfg.section_("Webtools")
        app = wconfig.application
        appconfig = cfg.section_(app)
        service = list(appconfig.views.active._internal_children)[0]
        dbsconfig = getattr(appconfig.views.active, service)

        config = Configuration()
        config.component_('DBS')
        config.DBS.application = app
        config.DBS.section_('model')
        config.DBS.model.object = dbsconfig.model.object 
        config.DBS.section_('formatter')
        config.DBS.formatter.object = dbsconfig.formatter.object
        config.DBS.database = dbsconfig.database
        config.DBS.dbowner = dbsconfig.dbowner
        config.DBS.version = dbsconfig.version
        return config

    def list1(self, call, params={}):
        """takes parameters as a dictionary"""
        return self.rest.default(*[call], **params)
    
    def list(self, *args, **kwargs):
        """
        takes individual parameters
        Example: api.list('files',dataset='/a/b/c')
        """
        return self.parseForException(self.rest.default(*args, **kwargs))

    def insert(self, call, params={}):
        request.method = 'POST'
        request.body = FileLike(params)
        return self.parseForException(self.rest.default(*[call]))

    def parseForException(self, data):
	if type(data)==type("abc"):
	    data=json.loads(data)	
	if type(data) == type({}) and data.has_key('exception'):
	    #print "Service Raised an exception: "+data['exception']
	    raise Exception("DBS Server raised an exception: " + data['message'])
	return data

def options():
    defaultcfg = os.environ["DBS_TEST_CONFIG_READER"]
    parser = OptionParser()
    parser.add_option("-c", "--config", dest="cfile", default=defaultcfg)
    parser.add_option("--primary_ds_name", dest='primary_ds_name')
    parser.add_option("--processed_ds_name", dest='processed_ds_name')
    parser.add_option("--data_tier_name", dest='data_tier_name')
    parser.add_option("--dataset", dest='dataset')
    parser.add_option("--block_name", dest='block_name')
    parser.add_option("--logical_file_name", dest='logical_file_name')
    parser.add_option("--site_name", dest='site_name')
    parser.add_option("--parent_dataset", dest='parent_dataset')
    parser.add_option("--version", dest='version')
    parser.add_option("--hash", dest='hash')
    parser.add_option("--app_name", dest='app_name')
    parser.add_option("--output_module_label", dest='output_module_label')
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
    api = DBSRestApi(params["cfile"])
    del params["cfile"]
    res = api.list1(call, params)
    dres = json.loads(res)
    pres = json.dumps(dres, sort_keys = True, indent = 4)
    print 
    print pres
