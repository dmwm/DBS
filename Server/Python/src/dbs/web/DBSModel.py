#!/usr/bin/env python
"""
DBS Rest Model module
"""

__revision__ = "$Id: DBSModel.py,v 1.4 2009/10/27 17:24:47 akhukhun Exp $"
__version__ = "$Revision: 1.4 $"


import re, json
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
        self.addService('GET', 'primds', self.listPrimaryDatasets, \
                        ['primdsname'])
        self.addService('GET', 'datasets', self.listDatasets, \
                        ['primdsname', 'procdsname', 'datatiername'])
        self.addService('PUT', 'primds', self.insertPrimaryDataset)
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
 
    def listPrimaryDatasets(self, primdsname = ""):
        """
        Example url's:
        http://dbs3/primds/
        http://dbs3/primds/qcd_20_30
        http://dbs3/primds/qcd*
        """
        data = {}
        primdsname = primdsname.replace("*","%")
        bo = DBSPrimaryDataset(self.logger, self.dbi)
        result = bo.listPrimaryDatasets(primdsname) 
        data.update({'result':result})
        return data
    
    def listDatasets(self, primdsname = "", procdsname = "", datatiername = ""):
        """
        Example url's:
        http://dbs3/datasets
        http://dbs3/datasets/RelVal*
        http://dbs3/datasets/RelVal*/*/*RECO
        """
        data = {}
        primdsname = primdsname.replace("*", "%")
        procdsname = procdsname.replace("*", "%")
        datatiername = datatiername.replace("*", "%")
        
        bo = DBSDataset(self.logger, self.dbi)
        result = bo.listDatasets(primdsname, procdsname, datatiername)
        data.update({'result':result})
        return data

    def insertPrimaryDataset(self):
        """
        gets the input from cherrypy request body.
        input must be a dictionary with the following two keys:
        primarydsname, primarydstype
        """
        body = request.body.read()
        indata = json.loads(body)

        #input validation
        pattern = re.compile("^[\w\d_-]*$")
        assert len(indata.keys()) == 2, "Invalid input, %s" %(str(indata))
        assert pattern.match(indata["primarydsname"]), \
            "Invalid character(s) in primarydsname: %s" % indata["primarydsname"]
        assert pattern.match(indata["primarydstype"]), \
            "Invalid character(s) in primarydstype: %s" % indata["primarydstype"]
        
        indata.update({"creationdate":123456, "createby":"me"})
        bo = DBSPrimaryDataset(self.logger, self.dbi)
        bo.insertPrimaryDataset(indata)
    
    def donothing(self, *args, **kwargs):
        """
        doing nothing
        """
        pass
