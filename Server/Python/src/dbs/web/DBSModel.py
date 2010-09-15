#!/usr/bin/env python
"""
DBS Rest Model module
"""

__revision__ = "$Id: DBSModel.py,v 1.3 2009/10/21 21:24:52 afaq Exp $"
__version__ = "$Revision: 1.3 $"


import re
try:
	# Python 2.6
	import json
except:
	# Prior to 2.6 requires simplejson
	import simplejson as json

from WMCore.WebTools.RESTModel import RESTModel
from dbs.business.DBSPrimaryDataset import DBSPrimaryDataset
from dbs.business.DBSDataset import DBSDataset
from cherrypy import request

class DBSModel(RESTModel):
    """
    DBS Rest Model class. 
    """
    def __init__(self, config):
        """
        All parameters are provided through DBSConfig module
        """
        RESTModel.__init__(self, config)
        self.methods = {'GET':{}, 'PUT':{}, 'POST':{}, 'DELETE':{}}
        self.addService('GET', 'listPrimaryDatasets', self.listPrimaryDatasets, ['primdsname'])
        self.addService('PUT', 'insertPrimaryDataset', self.insertPrimaryDataset)
        self.addService('GET', 'listDatasets', self.listDatasets)
        self.addService('POST', 'post', self.donothing)
        self.addService('DELETE', 'delete', self.donothing)


    def addService(self, verb, methodKey, func, args=[], validation=[], version=1):
        """
        method that adds services to the DBS rest model
        """
        self.methods[verb][methodKey] = {'args': args,
                                         'call': func,
                                         'validation': validation,
                                         'version': version}
        
    def listPrimaryDatasets(self, primdsname=""):
        """
        list primary dataset api. does not work for now.
        """
        data = {}
        self.sanitise_input(primdsname)  
        primdsname = primdsname.replace("*","%")
        bo = DBSPrimaryDataset(self.logger, self.dbi)
        result = bo.listPrimaryDatasets(primdsname) 
        data.update({'result':result})
        return data

    def listDatasets(self):
        """
        list dataset api.
        """
        data = {}
        bo = DBSDataset(self.logger, self.dbi)
        result = bo.listDatasets()
        data.update({'result':result})
        return data

    def insertPrimaryDataset(self):
        """
        gets the input from cherrypy request body.
        input must be a dictionary with the following two keys:
        primarydsname, primarydstype
        """
        body = request.body.read()
        input = json.loads(body)

        #input validation
        pattern = re.compile("^[\w\d_-]*$")
        assert len(input.keys()) == 2, "Invalid input, %s" %(str(input))
        assert pattern.match(input["primarydsname"]), \
            "Invalid character(s) in primarydsname: %s" % input["primarydsname"]
        assert pattern.match(input["primarydstype"]), \
            "Invalid character(s) in primarydstype: %s" % input["primarydstype"]
        
        input.update({"creationdate":123456, "createby":"me"})
        bo = DBSPrimaryDataset(self.logger, self.dbi)
        bo.insertPrimaryDataset(input)
    

    def insertDataset(self):
        """
        gets the input from cherrypy request body.
        """
        body = request.body.read()
        input = json.loads(body)

        #input validation
        pattern = re.compile("^[\w\d_-]*$")
        assert len(input.keys()) == 2, "Invalid input, %s" %(str(input))
        assert pattern.match(input["primarydsname"]), \
            "Invalid character(s) in primarydsname: %s" % input["primarydsname"]
        assert pattern.match(input["primarydstype"]), \
            "Invalid character(s) in primarydstype: %s" % input["primarydstype"]

        input.update({"creationdate":123456, "createby":"me"})
        bo = DBSPrimaryDataset(self.logger, self.dbi)
        bo.insertPrimaryDataset(input)


    def donothing(self, *args, **kwargs):
        """
        doing nothing
        """
        pass
