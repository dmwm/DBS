#!/usr/bin/env python
"""
DBS Reader Rest Model module
"""

__revision__ = "$Id: DBSReaderModel.py,v 1.30 2010/04/19 16:32:24 afaq Exp $"
__version__ = "$Revision: 1.30 $"

from WMCore.WebTools.RESTModel import RESTModel

from dbs.business.DBSPrimaryDataset import DBSPrimaryDataset
from dbs.business.DBSDataset import DBSDataset
from dbs.business.DBSBlock import DBSBlock
from dbs.business.DBSSite import DBSSite
from dbs.business.DBSFile import DBSFile
from dbs.business.DBSAcquisitionEra import DBSAcquisitionEra
from dbs.business.DBSOutputConfig import DBSOutputConfig
from dbs.business.DBSProcessingEra import DBSProcessingEra
from dbs.business.DBSRun import DBSRun
from dbs.business.DBSStorageElement import DBSStorageElement
from dbs.business.DBSDataType import DBSDataType
from dbs.business.DBSDataTier import DBSDataTier

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
        self.addService('GET', 'serverinfo', self.getServerInfo)
        self.addService('GET', 'primarydatasets', self.listPrimaryDatasets)#, ['primary_ds_name'])
        self.addService('GET', 'datasets', self.listDatasets)#, ['dataset', 'parent_dataset', 'release_version', 'pset_hash', 'app_name', 'output_module_label'])
        self.addService('GET', 'blocks', self.listBlocks)#, ['dataset', 'block_name', 'site_name'])
        self.addService('GET', 'files', self.listFiles)#, ['dataset', 'block_name', 'logical_file_name', 'release_version', 'pset_hash', 'app_name', 'output_module_label'])
        self.addService('GET', 'datasetparents', self.listDatasetParents)#, ['dataset'])
        self.addService('GET', 'datasetchildren', self.listDatasetChildren)#, ['dataset'])
        self.addService('GET', 'outputconfigs', self.listOutputConfigs)#, ['dataset', 'logical_file_name', 'release_version', 'pset_hash', 'app_name', 'output_module_label'])
        self.addService('GET', 'fileparents', self.listFileParents)#, ['logical_file_name'])
        self.addService('GET', 'filelumis', self.listFileLumis)#, ['logical_file_name', 'block_name'])
        self.addService('GET', 'runs', self.listRuns)#, ['dataset', 'block_name', 'logical_file_name', 'minrun', 'maxrun'])
        self.addService('GET', 'storage_elements', self.listStorageElements)#, ['block_name', 'se_name'])
        self.addService('GET', 'datatypes', self.listDataTypes)#, ['block_name', 'se_name'])
        self.addService('GET', 'datatiers', self.listDataTiers)#, ['block_name', 'se_name'])
        self.addService('GET', 'blockparents', self.listBlockParents)#, ['block_name', 'se_name'])
        self.addService('GET', 'blockchildren', self.listBlockChildren)#, ['block_name', 'se_name'])
        
        self.dbsPrimaryDataset = DBSPrimaryDataset(self.logger, self.dbi, config.dbowner)
        self.dbsDataset = DBSDataset(self.logger, self.dbi, config.dbowner)
        self.dbsBlock = DBSBlock(self.logger, self.dbi, config.dbowner)
        self.dbsFile = DBSFile(self.logger, self.dbi, config.dbowner)
        self.dbsAcqEra = DBSAcquisitionEra(self.logger, self.dbi, config.dbowner)
        self.dbsOutputConfig = DBSOutputConfig(self.logger, self.dbi, config.dbowner)
        self.dbsProcEra = DBSProcessingEra(self.logger, self.dbi, config.dbowner)
        self.dbsSite = DBSSite(self.logger, self.dbi, config.dbowner)
	self.dbsRun = DBSRun(self.logger, self.dbi, config.dbowner)
	self.dbsStorageElement = DBSStorageElement(self.logger, self.dbi, config.dbowner)
	self.dbsDataType = DBSDataType(self.logger, self.dbi, config.dbowner)
	self.dbsDataTier = DBSDataTier(self.logger, self.dbi, config.dbowner)

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


    def listPrimaryDatasets(self, primary_ds_name=""):
        """
        Example url's: <br />
        http://dbs3/primarydatasets <br />
        http://dbs3/primarydatasets/qcd_20_30 <br />
        http://dbs3/primarydatasets?primary_ds_name=qcd* <br />
        """
        primary_ds_name = primary_ds_name.replace("*","%")
        return self.dbsPrimaryDataset.listPrimaryDatasets(primary_ds_name)
        
    def listDatasets(self, dataset="", parent_dataset="", release_version="", pset_hash="", app_name="", output_module_label="", processing_version="", acquisition_era=""):
        """
        Example url's: <br />
        http://dbs3/datasets <br />
        http://dbs3/datasets/RelVal* <br />
        http://dbs3/datasets?dataset=/RelVal*/*/*RECO <br />
        http://dbs3/datasets?dataset=/RelVal*/*/*RECO&release_version=CMSSW_3_0_0<br />
        """
        dataset = dataset.replace("*", "%")
	parent_dataset = parent_dataset.replace("*", "%")
	release_version = release_version.replace("*", "%")
	pset_hash = pset_hash.replace("*", "%")
	app_name = app_name.replace("*", "%")
	output_module_label = output_module_label.replace("*", "%")
        return self.dbsDataset.listDatasets(dataset, parent_dataset, release_version, pset_hash, app_name, output_module_label, processing_version, acquisition_era)

    def listDataTiers(self, data_tier_name=""):
	"""
	Example url's:
	    http://dbs3/datatiers
	    http://dbs3/datatiers?data_tier_name=...
	"""
	data_tier_name = data_tier_name.replace("*","%")
	return self.dbsDataTier.listDataTiers(data_tier_name)
	
    def listBlocks(self, dataset="", block_name="", site_name=""):
        """
        Example url's:
        http://dbs3/blocks?dataset=/a/b/c <br />
        http://dbs3/blocks?block_name=/a/b/c%23*d <br />
        """
        dataset = dataset.replace("*","%")
        block_name = block_name.replace("*","%")
        return self.dbsBlock.listBlocks(dataset, block_name, site_name)

    def listBlockParents(self, block_name=""):
        """
        Example url's:
        http://dbs3/blockparents?block_name=/a/b/c%23*d <br />
        """
        block_name = block_name.replace("*","%")
        return self.dbsBlock.listBlockParents(block_name)
  
    def listBlockChildren(self, block_name=""):
        """
        Example url's:
        http://dbs3/blockchildren?block_name=/a/b/c%23*d <br />
        """
        block_name = block_name.replace("*","%")
        return self.dbsBlock.listBlockChildren(block_name)
 
    def listFiles(self, dataset = "", block_name = "", logical_file_name = "", release_version="", 
	pset_hash="", app_name="", output_module_label="", minrun=-1, maxrun=-1,
	origin_site=""):
        """
        Example url's: <br />
        http://dbs3/files?dataset=/a/b/c/ <br />
        http://dbs3/files?block_name=a/b/c#d <br />
        http://dbs3/files?dataset=/a/b/c&lfn=/store/* <br />
        http://dbs3/files?block_name=/a/b/c%23d&logical_file_name=/store/* <br />
        """
        logical_file_name = logical_file_name.replace("*", "%")
	release_version = release_version.replace("*", "%")
	pset_hash = pset_hash.replace("*", "%")
	app_name = app_name.replace("*", "%")
	output_module_label = output_module_label.replace("*", "%")
	if minrun!= -1 or maxrun!=-1:
	    return self.dbsFile.listFilesByRun(maxrun, minrun,  block_name=block_name, dataset=dataset)
	elif origin_site !="":
	    return self.dbsFile.listFileBySite(origin_site, block_name, dataset)
	else:
	    return self.dbsFile.listFiles(dataset, block_name, logical_file_name , release_version , pset_hash, app_name, output_module_label)
    
    def listDatasetParents(self, dataset):
        """
        Example url's <br />
        http://dbs3/datasetparents?dataset=/a/b/c
        """
        return self.dbsDataset.listDatasetParents(dataset)

    def listDatasetChildren(self, dataset):
        """
        Example url's <br />
        http://dbs3/datasetchildren?dataset=/a/b/c
        """
        return self.dbsDataset.listDatasetChildren(dataset)
    
    def listOutputConfigs(self, dataset="", logical_file_name="", release_version="", pset_hash="", app_name="", output_module_label=""):
        """
        Example url's: <br />
        http://dbs3/outputconfigurations <br />
        http://dbs3/outputconfigurations?dataset=a/b/c <br />
        http://dbs3/outputconfigurations?logical_file_name=lfn <br />
        http://dbs3/outputconfigurations?release_version=version <br />
        http://dbs3/outputconfigurations?pset_hash=hash <br/>
        http://dbs3/outputconfigurations?app_name=app_name <br/>
        http://dbs3/outputconfigurations?output_module_label="output_module_label" <br/>
        """
       	release_version = release_version.replace("*", "%")
	pset_hash = pset_hash.replace("*", "%")
	app_name = app_name.replace("*", "%")
	output_module_label = output_module_label.replace("*", "%")
	return self.dbsOutputConfig.listOutputConfigs(dataset, logical_file_name, release_version, pset_hash, app_name, output_module_label)
    
    def listFileParents(self, logical_file_name):
        """
        Example url's <br />
        http://dbs3/fileparents?logical_file_name=lfn
        """
        return self.dbsFile.listFileParents(logical_file_name)
        
    def listFileLumis(self, logical_file_name="", block_name=""):
        """
        Example url's <br />
        http://dbs3/filelumis?logical_file_name=lfn
        http://dbs3/filelumis?block_name=block_name
        """
        return self.dbsFile.listFileLumis(logical_file_name, block_name)
         
    def listRuns(self, dataset="", block_name="", logical_file_name="", minrun=-1, maxrun=-1):
        """
        Example url's <br />
        http://dbs3/runs?runmin=1&runmax=10
        http://dbs3/runs
	http://dbs3/runs?logical_file_name=lfn
	http://dbs3/runs?block_name=block_name
	http://dbs3/runs?dataset=dataset
        """
        return self.dbsRun.listRuns(dataset, block_name, logical_file_name , minrun, maxrun)
   
    def listStorageElements(self, block_name="", se_name=""):
        """
        Example url's <br />
        http://dbs3/storage_elements
	http://dbs3/storage_elements?block_name=block_name
	http://dbs3/storage_elements?se_name
        """
        return self.dbsStorageElements.listStorageElements(block_name, se_name)
  
    def listDataTypes(self, datatype="", dataset=""):
	"""
	lists datatypes known to dbs
	dataset : lists datatype of a dataset
	"""
	return  self.dbsDataType.listDataType(dataType=datatype, dataset=dataset)



	    
