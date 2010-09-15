#!/usr/bin/env python
"""
DBS Rest Model module
"""

__revision__ = "$Id: DBSModel.py,v 1.1 2009/11/27 09:55:03 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"

import re
import cjson

from cherrypy import request, response, HTTPError
from WMCore.WebTools.RESTModel import RESTModel

from dbs.business.DBSPrimaryDataset import DBSPrimaryDataset
from dbs.business.DBSDataset import DBSDataset
from dbs.business.DBSBlock import DBSBlock
from dbs.business.DBSFile import DBSFile

class DBSModel(RESTModel):
    """
    DBS3 Server API Documentation 
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
        self.addService('POST', 'primarydatasets', self.insertPrimaryDataset)
        self.addService('POST', 'datasets', self.insertDataset)
        self.addService('POST', 'blocks', self.insertBlock)
        self.addService('POST', 'files', self.insertFile)

        cdict = self.config.dictionary_()
        self.owner = cdict["dbowner"] 

        self.dbsPrimaryDataset = DBSPrimaryDataset(self.logger, self.dbi, self.owner)
        self.dbsDataset = DBSDataset(self.logger, self.dbi, self.owner)
        self.dbsBlock = DBSBlock(self.logger, self.dbi, self.owner)
        self.dbsFile = DBSFile(self.logger, self.dbi, self.owner)


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
        http://dbs3/primarydatasets/
        http://dbs3/primarydatasets/qcd_20_30
        http://dbs3/primarydatasets?primarydataset=qcd*
        """
        primds = primarydataset.replace("*","%")
        return self.dbsPrimaryDataset.listPrimaryDatasets(primds)
    
    def listDatasets(self, dataset = ""):
        """
        Example url's:
        http://dbs3/datasets
        http://dbs3/datasets/RelVal*
        http://dbs3/datasets?dataset=/RelVal*/*/*RECO
        """
        dataset = dataset.replace("*", "%")
        return self.dbsDataset.listDatasets(dataset = dataset)
        #return {}



    def listBlocks(self, dataset = "", block = ""):
        """
        Example url's:
        http://dbs3/blocks?dataset=/a/b/c
        http://dbs3/blocks?block=/a/b/c%23*d
        """
        block = block.replace("*","%")
        dataset = dataset.replace("*","%")
        return self.dbsBlock.listBlocks(dataset=dataset, block=block)
    
    def listFiles(self, dataset = "", block = "", lfn = ""):
        """
        Example url's:
        http://dbs3/files?dataset=/a/b/c/
        http://dbs3/files?block=a/b/c#d
        http://dbs3/files?dataset=/a/b/c&lfn=/store/*
        http://dbs3/files?block=/a/b/c%23d&lfn=/store/*
        """
        lfn = lfn.replace("*", "%")
        return self.dbsFile.listFiles(dataset = dataset, block = block, lfn = lfn)


       
    def insertPrimaryDataset(self):
        """
        gets the input from cherrypy request body.
        input must be a dictionary with the following two keys:
        PRIMARY_DS_NAME, PRIMARY_DS_TYPE
        """
        body = request.body.read()
        indata = cjson.decode(body)
        
        data = {}
        data.update({"creationdate":123456, "createby":"me"})
        data["primarydsname"] = indata["PRIMARY_DS_NAME"]
        data["primarydstype"] = indata["PRIMARY_DS_TYPE"]
        self.dbsPrimaryDataset.insertPrimaryDataset(data)
        

    def insertDataset(self):
        """
        gets the input from cherrypy request body.
        input must have the following keys:
        KEYS : required/optional:default = ...
        ...
        """

        body = request.body.read()
        indata = cjson.decode(body)
            
        # need proper validation
        dataset={}
        dataset['primaryds'] = indata['PRIMARY_DS_NAME']
        dataset['processedds'] = indata['PROCESSED_DATASET_NAME']
        dataset['datatier'] = indata['DATA_TIER_NAME']
        dataset['globaltag'] = indata.get('GLOBAL_TAG', '')
        dataset['physicsgroup'] = indata.get('PHYSICS_GROUP_NAME', '')
        dataset['creationdate'] = 1234
        dataset['createby'] = "me"
        dataset['datasettype'] = indata.get('DATASET_TYPE', 'test')
        dataset['lastmodificationdate'] = 1234
        dataset['lastmodifiedby'] = "me"
        dataset['isdatasetvalid'] = indata.get('IS_DATASET_VALID', '')
        dataset['xtcrosssection'] = indata.get('XTCROSSSECTION', '')
        dataset['dataset'] = indata["DATASET"]

        self.dbsDataset.insertDataset(dataset)

        
    def insertBlock(self):
        """
        gets the input from cherrypy request body.
        input must be a dictionary with the following keys:
        KEYS: required/optional : default = ...
        ...
        """

        body = request.body.read()
        indata = cjson.decode(body)

        # Proper validation needed
        vblock = re.match(r"(/[\w\d_-]+/[\w\d_-]+/[\w\d_-]+)#([\w\d_-]+)$", 
                      indata["BLOCK_NAME"])
        assert vblock, "Invalid block name %s" % indata["BLOCK_NAME"]
        block={} 
        block.update({
                      "dataset":vblock.groups()[0],
                      "creationdate": indata.get("CREATION_DATE", 123456),
                      "createby":indata.get("CREATE_BY","me"),
                      "lastmodificationdate":indata.get("LAST_MODIFICATION_DATE", 12345),
                      "lastmodifiedby":indata.get("LAST_MODIFIED_BY","me"),
                      "blockname":indata["BLOCK_NAME"],
                      "filecount":indata.get("FILE_COUNT", 0),
                      "blocksize":indata.get("BLOCK_SIZE", 0),
                      "originsite":"TEST",
                      "openforwriting":1
                      })
        self.dbsBlock.insertBlock(block)


    def insertFile(self):
        """
        gets the input from cherrypy request body
        input must be a (list of) dictionary with the following keys: <br />
        LOGICAL_FILE_NAME (required) : string  <br />
        IS_FILE_VALID: (optional, default = 1): 1/0 <br />
        BLOCK, required: /a/b/c#d <br />
        DATASET, required: /a/b/c <br />
        FILE_TYPE (optional, default = EDM): one of the predefined types, <br />
        CHECK_SUM (optional, default = '-1'): string <br />
        EVENT_COUNT (optional, default = -1): int <br />
        FILE_SIZE (optional, default = -1.): float <br />
        ADLER32 (optional, default = ''): string <br />
        MD5 (optional, default = ''): string <br />
        AUTO_CROSS_SECTION (optional, default = -1.): float <br />
	    FILE_LUMI_LIST (optional, default = []): [{"RUN_NUM": 123, "LUMI_SECTION_NUM": 12},{}....] <br />
	    FILE_PARENT_LIST(optional, default = []) :[{"FILE_PARENT_LFN": "mylfn"},{}....] <br />
        """
        body = request.body.read()
        indata = cjson.decode(body)["files"]
        
        # proper validation needed
        businput = []
        assert type(indata) in (list, dict)
        if type(indata) == dict:
            indata = [indata]
            
        for f in indata:
            f.update({"DATASET":f["DATASET"],
                     "CREATION_DATE":12345,
                     "CREATE_BY":"aleko",
                     "LAST_MODIFICATION_DATE":12345,
                     "LAST_MODIFIED_BY":"alsoaleko",
                     "FILE_LUMI_LIST":f.get("FILE_LUMI_LIST",[]),
                     "FILE_PARENT_LIST":f.get("FILE_PARENT_LIST",[])})
            businput.append(f)
            
        self.dbsFile.insertFile(businput)
