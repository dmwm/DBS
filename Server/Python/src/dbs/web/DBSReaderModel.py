#!/usr/bin/env python
"""
DBS Reader Rest Model module
"""

__revision__ = "$Id: DBSReaderModel.py,v 1.8 2009/12/27 13:39:17 akhukhun Exp $"
__version__ = "$Revision: 1.8 $"

from WMCore.WebTools.RESTModel import RESTModel

from dbs.business.DBSPrimaryDataset import DBSPrimaryDataset
from dbs.business.DBSDataset import DBSDataset
from dbs.business.DBSBlock import DBSBlock
from dbs.business.DBSFile import DBSFile
from dbs.business.DBSAcquisitionEra import DBSAcquisitionEra

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
        self.version = self.getServerVersion()
        
        self.methods = {'GET':{}, 'PUT':{}, 'POST':{}, 'DELETE':{}}
        self.addService('GET', 'primarydatasets', self.listPrimaryDatasets, ['primary_ds_name'])
        self.addService('GET', 'datasets', self.listDatasets, ['dataset'])
        self.addService('GET', 'blocks', self.listBlocks, ['dataset', 'block_name'])
        self.addService('GET', 'files', self.listFiles, ['dataset', 'block_name', 'logical_file_name'])
        self.addService('GET', 'serverinfo', self.getServerInfo)


        self.dbsPrimaryDataset = DBSPrimaryDataset(self.logger, self.dbi, config.dbowner)
        self.dbsDataset = DBSDataset(self.logger, self.dbi, config.dbowner)
        self.dbsBlock = DBSBlock(self.logger, self.dbi, config.dbowner)
        self.dbsFile = DBSFile(self.logger, self.dbi, config.dbowner)
        self.dbsAcqEra = DBSAcquisitionEra(self.logger, self.dbi, config.dbowner)
        
    def addService(self, verb, methodKey, func, args=[], validation=[], version=1):
        """
        method that adds services to the DBS rest model
        """
        self.methods[verb][methodKey] = {'args': args,
                                         'call': func,
                                         'validation': validation,
                                         'version': version}

    def getServerVersion(self):
        """
        Reading from __version__ tag, determines the version of the DBS Server
        """
        version = __server__version__.replace("$Name: ", "")
        version = version.replace("$", "")
        version = version.strip()
        return version
    
    
    def getServerInfo(self):
        """
        Method that provides information about DBS Server to the clients
        The information includes
        * Server Version - CVS Tag
        * Schema Version - Version of Schema this DBS instance is working with
        * ETC - TBD
        """
        ret = {}
        ret["version"] = self.getServerVersion()
        ret["schema"] = "DBS_0_0_0"
        return ret


    def listPrimaryDatasets(self, primary_ds_name = ""):
        """
        Example url's: <br />
        http://dbs3/primarydatasets/ <>
        http://dbs3/primarydatasets/qcd_20_30
        http://dbs3/primarydatasets?primary_ds_name=qcd*
        """
        primary_ds_name = primary_ds_name.replace("*","%")
        return self.dbsPrimaryDataset.listPrimaryDatasets(primary_ds_name)
        
    def listDatasets(self, dataset = ""):
        """
        Example url's: <br />
        http://dbs3/datasets <br />
        http://dbs3/datasets/RelVal* <br />
        http://dbs3/datasets?dataset=/RelVal*/*/*RECO <br />
        """
        dataset = dataset.replace("*", "%")
        return self.dbsDataset.listDatasets(dataset)

    def listBlocks(self, dataset = "", block_name = ""):
        """
        Example url's:
        http://dbs3/blocks?dataset=/a/b/c
        http://dbs3/blocks?block_name=/a/b/c%23*d
        """
        block_name = block_name.replace("*","%")
        dataset = dataset.replace("*","%")
        return self.dbsBlock.listBlocks(dataset, block_name)
    
    def listFiles(self, dataset = "", block_name = "", logical_file_name = ""):
        """
        Example url's:
        http://dbs3/files?dataset=/a/b/c/
        http://dbs3/files?block_name=a/b/c#d
        http://dbs3/files?dataset=/a/b/c&lfn=/store/*
        http://dbs3/files?block_name=/a/b/c%23d&logical_file_name=/store/*
        """
        logical_file_name = logical_file_name.replace("*", "%")
        return self.dbsFile.listFiles(dataset, block_name, logical_file_name)
