#!/usr/bin/env python
"""
DBS Rest Model module
"""

__revision__ = "$Id: DBSModel.py,v 1.10 2009/11/12 22:47:53 afaq Exp $"
__version__ = "$Revision: 1.10 $"

import re
import json, cjson
from WMCore.WebTools.RESTModel import RESTModel
from dbs.business.DBSPrimaryDataset import DBSPrimaryDataset
from dbs.business.DBSDataset import DBSDataset
from dbs.business.DBSBlock import DBSBlock
from dbs.business.DBSFile import DBSFile

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
        self.addService('GET', 'primarydatasets', self.listPrimaryDatasets, ['primarydataset'])
        self.addService('GET', 'datasets', self.listDatasets, ['dataset'])
        self.addService('GET', 'blocks', self.listBlocks, ['dataset', 'block'])
        self.addService('GET', 'files', self.listFiles, ['dataset', 'block', 'lfn'])
        self.addService('PUT', 'primarydatasets', self.insertPrimaryDataset)
        self.addService('PUT', 'datasets', self.insertDataset)
        self.addService('PUT', 'blocks', self.insertBlock)
        self.addService('PUT', 'files', self.insertFile)
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
 
    def listPrimaryDatasets(self, primarydataset = ""):
        """
        Example url's:
        http://dbs3/primds/
        http://dbs3/primds/qcd_20_30
        http://dbs3/primds/qcd*
        """
        data = {}
        primds = primarydataset.replace("*","%")
        bo = DBSPrimaryDataset(self.logger, self.dbi)
        data.update({'result':bo.listPrimaryDatasets(primds)})
        return data
    
    def listDatasets(self, dataset = ""):
        """
        Example url's:
        http://dbs3/datasets
        http://dbs3/datasets/RelVal*
        http://dbs3/datasets?dataset=/RelVal*/*/*RECO
        """
        dataset = dataset.replace("*", "%")
        bo = DBSDataset(self.logger, self.dbi)
        return {'result':bo.listDatasets(dataset = dataset)}
    
    def listBlocks(self, dataset = "", block = ""):
        """
        Example url's:
        http://dbs3/blocks?dataset=/a/b/c
        http://dbs3/blocks?block=/a/b/c%23*d
        """
        block = block.replace("*","%")
        dataset = dataset.replace("*","%")
        bo = DBSBlock(self.logger, self.dbi)
        return {"result":bo.listBlocks(dataset=dataset, block=block)}
    
    def listFiles(self, dataset = "", block = "", lfn = ""):
        """
        Example url's:
        http://dbs3/files?dataset=/a/b/c/
        http://dbs3/files?block=a/b/c#d
        http://dbs3/files?dataset=/a/b/c&lfn=/store/*
        http://dbs3/files?block=/a/b/c%23d&lfn=/store/*
        """
        bo = DBSFile(self.logger, self.dbi)
        lfn = lfn.replace("*", "%")
        result = bo.listFiles(dataset = dataset, block = block, lfn = lfn)
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
        vblock = re.match(r"(/[\w\d_-]+/[\w\d_-]+/[\w\d_-]+)#([\w\d_-]+)$", 
                          indata["blockname"])
        assert vblock, "Invalid block name %s" % indata["blockname"]
        assert type(indata["blocksize"]) == int
        assert type(indata["filecount"]) == int
        assert type(indata["openforwriting"]) == bool
        
        indata.update({"dataset":vblock.groups()[0],
                       "creationdate":123456,
                       "createby":"me",
                       "lastmodificationdate":12345,
                       "lastmodifiedby":"me"})
        bo = DBSBlock(self.logger, self.dbi)
        bo.insertBlock(indata)
        
    def insertFile(self):
        """
        gets the input from cherrypy request body
        input must be a (list of) dictionary with the following keys:
        logicalfilename: string
        isfilevalid: bool
        #dataset: /a/b/c I will get this from the block
        block: /a/b/c#d
        filetype: one of the predefined types, e.g. EDM,
        checksum: string
        eventcount: int
        filesize: float
        branchhash: string
        adler32: string
        md5: string
        autocrosssection: float
        """
        body = request.body.read()
        indata = cjson.decode(body)
        
        businput = []
        vblock = re.compile(r"(/[\w\d_-]+/[\w\d_-]+/[\w\d_-]+)#([\w\d_-]+)$")
        assert type(indata) in (list, dict)
        if type(indata) == dict:
            indata = [indata]
        for f  in indata:
            block = vblock.match(f["block"])
            conditions = (len(f.keys()) == 11,
                          "logicalfilename" in f.keys(),
                          type(f["isfilevalid"]) == bool,
                          block,
                          f["filetype"] in ("EDM"),
                           "checksum" in f.keys(),
                           type(f["eventcount"]) == int,
                           type(f["filesize"]) == float,
                           "branchhash" in f.keys(),
                           "adler32" in f.keys(),
                           "md5" in f.keys(),
                           type(f["autocrosssection"]) == float)
            for c in conditions:
                assert c, "One of the input conditions is not satisfied" % conditions
            f.update({"dataset":block.groups()[0],
                     "creationdate":12345,
                     "createby":"aleko",
                     "lastmodificationdate":1234,
                     "lastmodifiedby":"alsoaleko"})
            businput.append(f)
        bo = DBSFile(self.logger, self.dbi)
        bo.insertFile(businput)

        
    def donothing(self, *args, **kwargs):
        """
        doing nothing
        """
        pass
