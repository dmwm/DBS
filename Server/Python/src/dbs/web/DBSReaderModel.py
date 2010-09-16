#!/usr/bin/env python
"""
DBS Reader Rest Model module
"""

__revision__ = "$Id: DBSReaderModel.py,v 1.50 2010/08/13 20:38:37 yuyi Exp $"
__version__ = "$Revision: 1.50 $"

import cjson
import inspect
from WMCore.WebTools.RESTModel import RESTModel
from dbs.utils.dbsUtils import dbsUtils as DBSUtils 

from dbs.business.DBSPrimaryDataset import DBSPrimaryDataset
from dbs.business.DBSDataset import DBSDataset
from dbs.business.DBSBlock import DBSBlock
from dbs.business.DBSSite import DBSSite
from dbs.business.DBSFile import DBSFile
from dbs.business.DBSAcquisitionEra import DBSAcquisitionEra
from dbs.business.DBSOutputConfig import DBSOutputConfig
from dbs.business.DBSProcessingEra import DBSProcessingEra
from dbs.business.DBSRun import DBSRun
from dbs.business.DBSDataType import DBSDataType
from dbs.business.DBSDataTier import DBSDataTier
from dbs.business.DBSStatus import DBSStatus
from dbs.business.DBSMigrate import DBSMigrate

import urllib, urllib2
import re
import threading
import socket
import cjson

from cherrypy import server

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
	self.register()
        self.methods = {'GET':{}, 'PUT':{}, 'POST':{}, 'DELETE':{}}
	self.addMethod('GET', 'serverinfo', self.getServerInfo)
        self.addMethod('GET', 'primarydatasets', self.listPrimaryDatasets)
        self.addMethod('GET', 'datasets', self.listDatasets)
        self.addMethod('GET', 'blocks', self.listBlocks)
        self.addMethod('GET', 'files', self.listFiles)
        self.addMethod('GET', 'datasetparents', self.listDatasetParents)
        self.addMethod('GET', 'datasetchildren', self.listDatasetChildren)
        self.addMethod('GET', 'outputconfigs', self.listOutputConfigs)
        self.addMethod('GET', 'fileparents', self.listFileParents)
        self.addMethod('GET', 'filechildren', self.listFileChildren)
        self.addMethod('GET', 'filelumis', self.listFileLumis)
        self.addMethod('GET', 'runs', self.listRuns)
        self.addMethod('GET', 'sites', self.listSites)
        self.addMethod('GET', 'datatypes', self.listDataTypes)
        self.addMethod('GET', 'datatiers', self.listDataTiers)
        self.addMethod('GET', 'blockparents', self.listBlockParents)
        self.addMethod('GET', 'blockchildren', self.listBlockChildren)
        self.addMethod('GET', 'blockdump', self.dumpBlock)
        self.addMethod('GET', 'acquisitioneras', self.listAcquisitionEras)
        self.addMethod('GET', 'processingeras', self.listProcessingEras)
	self.addMethod('GET', 'help', self.getHelp)
	self.addMethod('GET', 'register', self.register)

        self.dbsPrimaryDataset = DBSPrimaryDataset(self.logger, self.dbi, config.dbowner)
        self.dbsDataset = DBSDataset(self.logger, self.dbi, config.dbowner)
        self.dbsBlock = DBSBlock(self.logger, self.dbi, config.dbowner)
        self.dbsFile = DBSFile(self.logger, self.dbi, config.dbowner)
        self.dbsAcqEra = DBSAcquisitionEra(self.logger, self.dbi, config.dbowner)
        self.dbsOutputConfig = DBSOutputConfig(self.logger, self.dbi, config.dbowner)
        self.dbsProcEra = DBSProcessingEra(self.logger, self.dbi, config.dbowner)
        self.dbsSite = DBSSite(self.logger, self.dbi, config.dbowner)
	self.dbsRun = DBSRun(self.logger, self.dbi, config.dbowner)
	self.dbsDataType = DBSDataType(self.logger, self.dbi, config.dbowner)
	self.dbsDataTier = DBSDataTier(self.logger, self.dbi, config.dbowner)
	self.dbsStatus = DBSStatus(self.logger, self.dbi, config.dbowner)
	self.dbsMigrate = DBSMigrate(self.logger, self.dbi, config.dbowner)

	self.dbsUtils = DBSUtils()
    
    def geoLocateThisHost(self, ip):
	"""
	Locate the host, otherwise return 'UNKNOWN'
	"""
	response = urllib.urlopen('http://api.hostip.info/get_html.php?ip=%s' % ip).read()
	m = re.search('City: (.*)', response)
	if m:
	    return m.group(1)
	return "UNKNOWN"
    
    def register(self):
	"""
	Method that attempts to register this service with Service Registry.
	NO Error is thrown, if the registry is inaccessible for any reason
	"""
	try:
	    srvcregistry="http://cmssrv18.fnal.gov:8686/SRVCREGISTRY/services"
	    addthis={}
	    addthis['NAME'] = self.config._internal_name
	    addthis['TYPE'] = self.__class__.__name__
	    addthis['LOCATION'] = self.geoLocateThisHost(socket.gethostbyname(socket.gethostname()))
	    addthis['STATUS'] = "WORKING"
	    addthis['ADMIN'] = self.config.admin
	    addthis['URI'] = "%s/%s" % (server.base(), self.config._internal_name)
	    addthis['DB'] = self.config.database.connectUrl  #<<<<<<<<<<<remove password
	    addthis['VERSION'] = self.getServerVersion()
	    addthis['LAST_CONTACT'] = dbsUtils().getTime()
	    addthis['COMMENTS'] = "DBS Service"
	    self.logger.warning("REGISTERING DBS: %s" %str(addthis))
	    params = cjson.encode(addthis)
	    headers =  {"Content-type": "application/json", "Accept": "application/json" }
	    self.opener =  urllib2.build_opener()
	    req = urllib2.Request(url=srvcregistry, data=params, headers = headers)
	    req.get_method = lambda: 'POST'
	    data = self.opener.open(req)
	except Exception, ex:
	    print ex
	    pass

    def getServerVersion(self):
        """
        Reading from __version__ tag, determines the version of the DBS Server
        """
        version = __server__version__.replace("$Name: ", "")
        version = version.replace("$", "")
        version = version.strip()
        return version

    def getHelp(self, call=""):
	if call:
	    params=inspect.getargspec(self.methods['GET'][call]['call'])[0]
	    del params[params.index('self')]
	    doc = self.methods['GET'][call]['call'].__doc__
	    return dict(params=params, doc=doc)
	else:
	    return self.methods['GET'].keys()
    
    def getServerInfo(self):
        """
        Method that provides information about DBS Server to the clients
        The information includes
        * Server Version - CVS Tag
        * Schema Version - Version of Schema this DBS instance is working with
        * ETC - TBD
        """
        ret = {}
        ret["tagged_version"] = self.getServerVersion()
        ret["schema"] = self.dbsStatus.getSchemaStatus()
	ret["components"] = self.dbsStatus.getComponentStatus()
        return ret

    def listPrimaryDatasets(self, primary_ds_name="", primary_ds_type=""):
        """
        Example url's: <br />
        http://dbs3/primarydatasets <br />
        http://dbs3/primarydatasets/qcd_20_30 <br />
        http://dbs3/primarydatasets?primary_ds_name=qcd* <br />
	http://dbs3/primarydatasets?primary_ds_type=qcd* <br />
        """
        primary_ds_name = primary_ds_name.replace("*","%")
	primary_ds_type = primary_ds_type.replace("*","%")
        return self.dbsPrimaryDataset.listPrimaryDatasets(primary_ds_name, primary_ds_type)
        
    def listDatasets(self, dataset="", parent_dataset="", release_version="", pset_hash="", app_name="", output_module_label="", 
			processing_version="", acquisition_era="", run_num=0, physics_group_name="", logical_file_name="", primary_ds_name="",
			primary_ds_type="", data_tier_name="", dataset_access_type="", detail=False):
        """
	This API lists the dataset paths and associated information.
	If no parameter is given, all datasets will be returned.
	<dataset> parameter can include one or several '*' as wildcards.
	<detail> parameter is defaulted to False, which means only dataset paths will be returned in the output dictionary. 
	In order to get more information, one needs to provide detail=True.
	<run_num> can be only be passed as a single number. No interval of run numbers is supported for this api for now.
        """
        dataset = dataset.replace("*", "%")
	parent_dataset = parent_dataset.replace("*", "%")
	release_version = release_version.replace("*", "%")
	pset_hash = pset_hash.replace("*", "%")
	app_name = app_name.replace("*", "%")
	output_module_label = output_module_label.replace("*", "%")
	logical_file_name = logical_file_name.replace("*", "%")
	physics_group_name = physics_group_name.replace("*", "%")
	primary_ds_name = primary_ds_name.replace("*", "%")
	primary_ds_type = primary_ds_type.replace("*", "%")
	data_tier_name = data_tier_name.replace("*", "%")
	dataset_access_type = dataset_access_type.replace("*", "%")
	run_num = int(run_num)
	detail = detail in (True, 1, "True", "1")

	return self.dbsDataset.listDatasets(dataset, parent_dataset, release_version, pset_hash, app_name, output_module_label, processing_version, acquisition_era,
	    run_num, physics_group_name, logical_file_name, primary_ds_name, primary_ds_type, data_tier_name, dataset_access_type, detail)

    def listDataTiers(self, data_tier_name=""):
	"""
	Example url's:
	    http://dbs3/datatiers
	    http://dbs3/datatiers?data_tier_name=...
	"""
	data_tier_name = data_tier_name.replace("*","%")
	return self.dbsDataTier.listDataTiers(data_tier_name)
	
    def listBlocks(self, dataset="", block_name="", origin_site_name="", logical_file_name="",run_num=-1, detail=False):
        """
        Example url's:
        http://dbs3/blocks?dataset=myDataset ||?origin_site_name=mySite <br />
        http://dbs3/blocks?block_name=myBlock ||?origin_site_name=mySite <br />
	http://dbs3/blocks?logical_file_name=my_lfn ||?origin_site_name=mySite<br />
	http://dbs3/blocks?logical_file_name=my_lfn*?dataset=myDataset*?block_name=myBlock ||?origin_site_name=mySite<br />
        """
	#site_name is ORIGIN_SITE_NAME. We need to change the name and add REAL site_name
        dataset = dataset.replace("*","%")
        block_name = block_name.replace("*","%")
	logical_file_name = logical_file_name.replace("*","%")
	origin_site_name = origin_site_name.replace("*","%")
	run_num = int(run_num)
	detail = detail in (True, 1, "True", "1")
        return self.dbsBlock.listBlocks(dataset, block_name, origin_site_name, logical_file_name,run_num, detail)

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
	origin_site_name="", lumi_list="", detail=False):
        """
	This API returns logical file names and associated information.
	One of the following three parameters must be provided: dataset, block, logical_file_name.
	<detail> parameter is defaulted to False, which means only logical_file_names will be returned in the output json. 
	In order to get more information, one needs to provide detail=True.
	Run numbers must be passed as two parameters, minrun and maxrun. 
	for lumi_list the following two json formats are supported:
	    - '[a1, a2, a3,]' 
	    - '[[a,b], [c, d],]'
	Also if lumi_list is provided, one also needs to provide both minrun and maxrun parameters(equal) 
	No POST/PUT call for run-lumi json combination is provided as input for now...
        """
        logical_file_name = logical_file_name.replace("*", "%")
	release_version = release_version.replace("*", "%")
	pset_hash = pset_hash.replace("*", "%")
	app_name = app_name.replace("*", "%")
	block_name = block_name.replace("*", "%")
	origin_site_name = origin_site_name.replace("*", "%")
	dataset = dataset.replace("*", "%")
	maxrun = int(maxrun)
	minrun = int(minrun)
	if lumi_list:
	    #lumi_list = cjson.decode(lumi_list)
	    lumi_list = self.dbsUtils.decodeLumiIntervals(lumi_list)
	detail = detail in (True, 1, "True", "1")
	output_module_label = output_module_label.replace("*", "%")
	return self.dbsFile.listFiles(dataset, block_name, logical_file_name , release_version , pset_hash, app_name, 
					output_module_label, maxrun, minrun, origin_site_name, lumi_list, detail)
    
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
    
    def listOutputConfigs(self, dataset="", logical_file_name="", release_version="", pset_hash="", app_name="",
    output_module_label="", block_id=0):
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
	return self.dbsOutputConfig.listOutputConfigs(dataset, logical_file_name, release_version, pset_hash, app_name,
	output_module_label, block_id)
    
    def listFileParents(self, logical_file_name):
        """
        Example url's <br />
        http://dbs3/fileparents?logical_file_name=lfn
        """
        return self.dbsFile.listFileParents(logical_file_name)

    def listFileChildren(self, logical_file_name):
        """
        Example url's <br />
        http://dbs3/filechildren?logical_file_name=lfn
        """
        return self.dbsFile.listFileChildren(logical_file_name)
       

    def listFileLumis(self, logical_file_name="", block_name=""):
        """
        Example url's <br />
        http://dbs3/filelumis?logical_file_name=lfn
        http://dbs3/filelumis?block_name=block_name
        """
        return self.dbsFile.listFileLumis(logical_file_name, block_name)
         
    def listRuns(self, minrun=-1, maxrun=-1, logical_file_name="", block_name="", dataset=""):
        """
        http://dbs3/runs?runmin=1&runmax=10
        http://dbs3/runs
        """
	if(logical_file_name):
	    logical_file_name= logical_file_name.replace("*", "%")
	if(block_name):
	    block_name = block_name.replace("*", "%")
	if(dataset):
	    dataset = dataset.replace("*", "%")
        return self.dbsRun.listRuns(minrun, maxrun, logical_file_name, block_name, dataset)
   
    def listSites(self, block_name="", site_name=""):
        """
        Example url's <br />
        http://dbs3/sites
	http://dbs3/sites?block_name=block_name
	http://dbs3/sites?site_name=T1_FNAL
        """
        return self.dbsSite.listSites(block_name, site_name)
  
    def listDataTypes(self, datatype="", dataset=""):
	"""
	lists datatypes known to dbs
	dataset : lists datatype of a dataset
	"""
	return  self.dbsDataType.listDataType(dataType=datatype, dataset=dataset)

    def dumpBlock(self, block_name):
	"""
	Returns all information related with the block_name
	"""
	return self.dbsMigrate.dumpBlock(block_name)

    def listAcquisitionEras(self):
	"""
	lists acquisition eras known to dbs
	"""
	return  self.dbsAcqEra.listAcquisitionEras()

    def listProcessingEras(self):
	"""
	lists acquisition eras known to dbs
	"""
	return  self.dbsProcEra.listProcessingEras()


	    
