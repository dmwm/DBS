#!/usr/bin/env python
"""
DBS Rest Model module
"""

__revision__ = "$Id: DBSModel.py,v 1.7 2009/10/30 16:54:26 akhukhun Exp $"
__version__ = "$Revision: 1.7 $"


import re, json
from WMCore.WebTools.RESTModel import RESTModel
from dbs.business.DBSPrimaryDataset import DBSPrimaryDataset
from dbs.business.DBSDataset import DBSDataset
from dbs.business.DBSBlock import DBSBlock

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
                        ['primds'])
        self.addService('GET', 'datasets', self.listDatasets, \
                        ['primds', 'procds', 'tier'])
        self.addService('GET', 'blocks', self.listBlocks, \
                        ['primds', 'procds', 'tier', 'block'])
        self.addService('PUT', 'primds', self.insertPrimaryDataset)
        self.addService('PUT', 'datasets', self.insertDataset)
        self.addService('PUT', 'blocks', self.insertBlock)
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
 
    def listPrimaryDatasets(self, primds = ""):
        """
        Example url's:
        http://dbs3/primds/
        http://dbs3/primds/qcd_20_30
        http://dbs3/primds/qcd*
        """
        data = {}
        primds = primds.replace("*","%")
        bo = DBSPrimaryDataset(self.logger, self.dbi)
        result = bo.listPrimaryDatasets(primds) 
        data.update({'result':result})
        return data
    
    def listDatasets(self, primds = "", procds = "", tier = ""):
        """
        Example url's:
        http://dbs3/datasets
        http://dbs3/datasets/RelVal*
        http://dbs3/datasets/RelVal*/*/*RECO
        """
        primds = primds.replace("*", "%")
        procds = procds.replace("*", "%")
        tier = tier.replace("*", "%")
        
        bo = DBSDataset(self.logger, self.dbi)
        result = bo.listDatasets(primds, procds, tier)
        return {'result':result}
    
    def listBlocks(self, primds, procds, tier, block=""):
        """
        Example url's:
        http://dbs3/blocks/a/b/c
        http://dbs3/blocks/a/b/c/s*df
        """
        block = block.replace("*","%")
        bo = DBSBlock(self.logger, self.dbi)
        dataset = "/".join(("/%s", "%s", "%s"))%(primds, procds, tier)
        if not block == "": 
            block = dataset + "#" + block
        result = bo.listBlocks(dataset, block)
        return {"result":result}

    def insertPrimaryDataset(self):
        """
        gets the input from cherrypy request body.
        input must be a dictionary with the following two keys:
        primarydsname, primarydstype
        """
        body = request.body.read()
        indata = json.loads(body)

        #input validation
        valid = re.compile("^[\w\d_-]+$")
        assert type(indata) == dict
        assert len(indata.keys()) == 2, "Invalid input, %s" %(str(indata))
        assert valid.match(indata["primarydsname"]), \
            "Invalid character(s) in primarydsname: %s" % indata["primarydsname"]
        assert valid.match(indata["primarydstype"]), \
            "Invalid character(s) in primarydstype: %s" % indata["primarydstype"]
        
        indata.update({"creationdate":123456, "createby":"me"})
        bo = DBSPrimaryDataset(self.logger, self.dbi)
        bo.insertPrimaryDataset(indata)
    
    def insertDataset(self):
        """
        gets the input from cherrypy request body.
        input must have the following keys:
        dataset, isdatasetvalid
        datasettype, acquisitionera, processingversion,
        physicsgroup, xtcrosssection, globaltag
        """
        body = request.body.read()
        indata = json.loads(body)
        
        #>> validation.. will be moved to a separate method probably
        assert type(indata) == dict
        assert len(indata.keys()) == 8
        vds = re.match(r"/([\w\d_-]+)/([\w\d_-]+)/([\w\d_-]+)", indata["dataset"])
        assert vds
        assert type(indata["isdatasetvalid"]) == bool
        assert type(indata["xtcrosssection"]) == float
        #finish validation
        #<< end of validation
        
        indata.update({"primaryds":vds.groups()[0],
                      "processedds":vds.groups()[1],
                      "datatier":vds.groups()[2],
                      "creationdate":1234,
                      "createby":"me",
                      "lastmodificationdate":1234,
                      "lastmodifiedby":"alsome"})
        
        bo = DBSDataset(self.logger, self.dbi)
        bo.insertDataset(indata)
                
         
        
        
    def insertBlock(self):
        """
        gets the input from cherrypy request body.
        input must be a dictionary with the following keys:
        blockname:/string/string/string#string
        openforwriting bool
        originsite string
        blocksize int
        filecount int
        """
        body = request.body.read()
        indata = json.loads(body)
        
        #>>validation - will be moved to the separate method later.
        assert type(indata) == dict
        assert len(indata.keys()) == 5
        assert "blockname" in indata.keys()
        validblockname = re.compile("^/[\w\d_-]+/[\w\d_-]+/[\w\d_-]+#[\w\d_-]+$")
        assert validblockname.match(indata["blockname"])
        assert type(indata["blocksize"]) == int
        assert type(indata["filecount"]) == int
        assert type(indata["openforwriting"]) == bool
        
        #<<end of validation
        dlist = indata["blockname"].split("#")
        indata.update({"dataset":dlist[0],
                       "creationdate":123456,
                       "createby":"me",
                       "lastmodificationdate":12345,
                       "lastmodifiedby":"me"})
        #<<possible end of the validation 
        bo = DBSBlock(self.logger, self.dbi)
        bo.insertBlock(indata)
        
        
    def donothing(self, *args, **kwargs):
        """
        doing nothing
        """
        pass
