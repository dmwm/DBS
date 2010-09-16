"""
This module provides a stand-alone client for DBS server
Also DBSRestApi will be used in various stand-alone tests
"""

__revision__ = "$Id: DBSRestApi.py,v 1.17 2010/08/27 21:57:13 afaq Exp $"
__version__ = "$Revision: 1.17 $"

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
    def __init__(self, configfile, service='DBS'):
        log.error_log.setLevel(logging.ERROR)
        config = self.configure(configfile, service)
        config = config.section_("DBS")
        self.rest = RESTApi(config)
        self.config = config

    def configure(self, configfile, service):
        cfg = loadConfigurationFile(configfile)
        wconfig = cfg.section_("Webtools")
        app = wconfig.application
        appconfig = cfg.section_(app)
        dbsconfig = getattr(appconfig.views.active, service)
	databasecore = cfg.CoreDatabase
	
	# Eitehr we change formatter 
	# OR change the 'Accept' type to application/json (which we don't know how to do at thi moment)	
	dbsconfig.formatter.object="WMCore.WebTools.RESTFormatter"
        config = Configuration()

	config.section_("CoreDatabase")
	config.CoreDatabase = databasecore
	
        config.component_('DBS')
        config.DBS.application = app
	config.DBS.model       = dbsconfig.model
	config.DBS.formatter   = dbsconfig.formatter
        config.DBS.version     = dbsconfig.version
	config.DBS.default_expires = 300
	# DBS uses owner name, directly from app section at the moment (does not pick it from CoreDatabse)
	config.DBS.dbowner     = databasecore.dbowner
	# Add the CoreDatabase section to DBS
	config.DBS.database = config.CoreDatabase
	
	
        return config

    def list1(self, call, params={}):
        """takes parameters as a dictionary"""
        request.method = 'GET'
        return self.rest.default(*[call], **params)
    
    def list(self, *args, **kwargs):
        """
        takes individual parameters
        Example: api.list('files',dataset='/a/b/c')
        """
	#print "List API call ....."
        request.method = 'GET'
        return self.parseForException(self.rest.default(*args, **kwargs))

    def insert(self, call, params={}):
        request.method = 'POST'
        request.body = FileLike(params)
	#Forcing NO use of insert buffer during the unit tests
	if call=='files':
	    ret=self.rest.default(*[call, False])
	ret=self.rest.default(*[call])
        return self.parseForException(ret)

    def update(self, *args, **kwargs):
        request.method = 'PUT'
        ret=self.rest.default(*args, **kwargs)
        return self.parseForException(ret)

    def parseForException(self, data):
	if type(data)==type("abc"):
	    data=json.loads(data)	
	if type(data) == type({}):
	    if type(data) == type({}) and data.has_key('exception'):
		raise Exception("DBS Server raised an exception: " + (data['message']))
	return data

def options():
    defaultcfg = os.environ["DBS_TEST_CONFIG_READER"]
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
    parser.add_option("--version", dest='version')
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
    print 
    print pres
