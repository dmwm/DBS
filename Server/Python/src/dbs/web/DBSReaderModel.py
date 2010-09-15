#!/usr/bin/env python
"""
DBS Reader Rest Model module
"""

__revision__ = "$Id: DBSReaderModel.py,v 1.2 2009/12/09 17:08:44 afaq Exp $"
__version__ = "$Revision: 1.2 $"

import re
import cjson
import time
import hashlib
import cherrypy
from cherrypy import request, response, HTTPError
from WMCore.WebTools.RESTModel import RESTModel

from dbs.business.DBSPrimaryDataset import DBSPrimaryDataset
from dbs.business.DBSDataset import DBSDataset
from dbs.business.DBSBlock import DBSBlock
from dbs.business.DBSFile import DBSFile

#DBS Api version, set from the CVS checkout tag, for HEAD version, set it in dbs.config
__server__version__ = "$Name:  $"

class DBSReaderModel(RESTModel):
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

        cdict = self.config.dictionary_()
	print cdict
        self.owner = cdict["dbowner"] 

        self.dbsPrimaryDataset = DBSPrimaryDataset(self.logger, self.dbi, self.owner)
        self.dbsDataset = DBSDataset(self.logger, self.dbi, self.owner)
        self.dbsBlock = DBSBlock(self.logger, self.dbi, self.owner)
        self.dbsFile = DBSFile(self.logger, self.dbi, self.owner)

    def set_expire(self):
            "We can add Cacche control here"
            timestamp = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
            cherrypy.response.headers['Expires'] = timestamp
            #cherrypy.response.headers['Cache-control'] = 'no-cache'

    def getHash(self, instr):
	m = hashlib.md5()
	m.update(str(instr))
	return m.hexdigest()

    def getServerVersion(self):
	"""
    	Reading from __version__ tag, determines the version of the DBS Server
    	"""
    	version = __server__version__.replace("$Name: ", "")
    	version = version.replace("$", "")
    	version = version.strip()
    	return version

    def returnDAS(self, intime, inparams, api, result):
	ret={"DBS":{
		"request_timestamp" : intime,
		"request_url" : "http://host:port/path",
		"request_method" : "GET",
		"request_params" : inparams,
		"response_version" : self.getServerVersion(),
		"response_expires" : 300, 
		"response_checksum" : self.getHash(result),
		"request_api" : api,
		"call_time" : time.time()-intime,
		}
	}
	ret["DBS"][api]=result
	return ret

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
	self.set_expire()
	intime=time.time()
        primds = primarydataset.replace("*","%")
        ret = self.dbsPrimaryDataset.listPrimaryDatasets(primds)
	return self.returnDAS(intime, primds, "listPrimaryDatasets", ret)
		
    def listDatasets(self, dataset = ""):
        """
        Example url's:
        http://dbs3/datasets
        http://dbs3/datasets/RelVal*
        http://dbs3/datasets?dataset=/RelVal*/*/*RECO
        """
	intime=time.time()
        dataset = dataset.replace("*", "%")
	ret = self.dbsDataset.listDatasets(dataset = dataset)
        return self.returnDAS(intime, dataset, "listDatasets", ret)
        #return {}

    def listBlocks(self, dataset = "", block = ""):
        """
        Example url's:
        http://dbs3/blocks?dataset=/a/b/c
        http://dbs3/blocks?block=/a/b/c%23*d
        """
	intime=time.time()
        block = block.replace("*","%")
        dataset = dataset.replace("*","%")
        ret=self.dbsBlock.listBlocks(dataset=dataset, block=block)
	#FIXME Assuming user passed dataset for now
	return self.returnDAS(intime, dataset, "listBlocks", ret)
    
    def listFiles(self, dataset = "", block = "", lfn = ""):
        """
        Example url's:
        http://dbs3/files?dataset=/a/b/c/
        http://dbs3/files?block=a/b/c#d
        http://dbs3/files?dataset=/a/b/c&lfn=/store/*
        http://dbs3/files?block=/a/b/c%23d&lfn=/store/*
        """
	intime=time.time()
        lfn = lfn.replace("*", "%")
        ret = self.dbsFile.listFiles(dataset = dataset, block = block, lfn = lfn)
	#FIXME Assuming user passed dataset for now
        return self.returnDAS(intime, dataset, "listFiles", ret)
        #return [r for r in result]
        #for r in result:
        #    yield cjson.encode(r)


