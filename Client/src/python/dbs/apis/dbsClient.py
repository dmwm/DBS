# 
# $Revision: 1.54 $"
# $Id: dbsClient.py,v 1.54 2010/08/16 18:43:15 afaq Exp $"
# @author anzar
#
import os, sys, socket
import urllib, urllib2
from httplib import HTTPConnection
from StringIO import StringIO
from exceptions import Exception
import cjson

try:
	# Python 2.6
	import json
except:
	# Prior to 2.6 requires simplejson
	import simplejson as json

class DbsApi:
        def __init__(self, url="", proxy=""):
		"""
		* DbsApi CTOR
		url: serevr URL
		proxy: http proxy; this feature is TURNED OFF at the moemnt
		"""
                self.url=url
		self.proxy=proxy
		self.opener =  urllib2.build_opener()

	def callServer(self, urlplus="", params={}, callmethod='GET'):
		"""
        	* callServer
		* API to make HTTP call to the DBS Server
		* urlplus: addition to URL, this is generall where the VERB is provided for the REST call, as '/files'
        	* params: parameters to server
		* callmethod; the HTTP method used, by default it is HTTP-GET, possible values are GET, POST and PUT
		"""
		UserID=os.environ['USER']+'@'+socket.gethostname()
		headers =  {"Content-type": "application/json", "Accept": "application/json", "UserID": UserID }

		res=""
		try:
			calling=self.url+urlplus
			proxies = {}
			if self.proxy not in (None, ""):
				proxies = { 'http': self.proxy }
			#print calling
			if params == {} and not callmethod in ('POST', 'PUT') :
				#data = urllib.urlopen(calling, proxies=proxies)
				#data = urllib2.urlopen(calling)
				req = urllib2.Request(url=calling, headers = headers)
				data = urllib2.urlopen(req)
			else:
				params = cjson.encode(params)
				req = urllib2.Request(url=calling, data=params, headers = headers)
				req.get_method = lambda: callmethod
				data = self.opener.open(req)
			res = data.read()
		except urllib2.HTTPError, httperror:
			self.parseForException(json.loads(httperror.read()))
			
			#HTTPError(req.get_full_url(), code, msg, hdrs, fp)
		except urllib2.URLError, urlerror:
			raise urlerror
		
		#FIXME: We will always return JSON from DBS, even from POST, PUT, DELETE APIs, make life easy here
		json_ret=json.loads(res)
		self.parseForException(json_ret)
		return json_ret
		
	def parseForException(self, data):
	    """
	    An internal method, should not be used by clients
	    """
	    if type(data)==type("abc"):
		data=json.loads(data)
            if type(data) == type({}) and data.has_key('exception'):
		#print "Service Raised an exception: "+data['exception']
		raise Exception("DBS Server raised an exception: %s" %data['message'])
	    return data
		
        def serverinfo(self):
                """
                * API to retrieve DAS interface and status information
		* can be used a PING
                """
                return self.callServer("/serverinfo")

	def listPrimaryDatasets(self, dataset=""):
		"""
		* API to list ALL primary datasets in DBS 
		* dataset: If provided, will list THAT primary dataset
		"""
		if dataset:
		    return self.callServer("/primarydatasets?primary_ds_name=%s" %dataset )
		return self.callServer("/primarydatasets")

	def listAcquisitionEras(self, dataset=""):
		"""
		* API to list ALL Acquisition Eras in DBS 
		"""
		return self.callServer("/acquisitioneras")

        def listProcessingEras(self, dataset=""):
                """
                * API to list ALL Processing Eras in DBS 
                """
                return self.callServer("/processingeras")

        def insertPrimaryDataset(self, primaryDSObj={}):
                """
                * API to insert A primary dataset in DBS 
                * primaryDSObj : primary dataset object of type {} , with key(s) :-
			* primary_ds_type : TYPE (out of valid types in DBS, MC, DATA) --REQUIRED
			* primary_ds_name : Name of the primary dataset --REQUIRED
                """
                return self.callServer("/primarydatasets", params = primaryDSObj, callmethod='POST' )

	def insertOutputConfig(self, outputConfigObj={}):
                """
                * API to insert An OutputConfig in DBS 
                * outputConfigObj : Output Config object of type {}, with key(s) :-
			    * app_name : App Name  --REQUIRED
			    * release_version : Release Version --REQUIRED
			    * pset_hash : Pset Hash --REQUIRED
			    * output_module_label : Output Module Label --REQUIRED
                """
                return self.callServer("/outputconfigs", params = outputConfigObj , callmethod='POST' )

        def listOutputConfigs(self, dataset="", logical_file_name="", release_version="", pset_hash="", app_name="", output_module_label=""):
                """
                * API to list OutputConfigs in DBS 
                * dataset : Full dataset (path) of the dataset
		* parent_dataset : Full dataset (path) of the dataset
		* release_version : cmssw version
		* pset_hash : pset hash
		* app_name : Application name (generally it is cmsRun)
		* output_module_label : output_module_label
		* 
		* You can use ANY combination of these parameters in this API
		* All parameters are optional, if you do not provide any parameter, ALL configs will be listed from DBS
                """
		add_to_url=""
		amp=False
		if dataset: 
		    add_to_url += "dataset=%s"%dataset
		    amp=True
		if release_version:
		    if amp: add_to_url += "&"
		    add_to_url += "release_version=%s"%release_version
		    amp=True
		if pset_hash:
		    if amp: add_to_url += "&"
		    add_to_url += "pset_hash=%s"%pset_hash
		    amp=True
		if app_name:
		    if amp: add_to_url += "&"
		    add_to_url += "app_name=%s"%app_name
		    amp=True
		if  output_module_label:
		    if amp: add_to_url += "&"
		    add_to_url += "output_module_label=%s"%output_module_label
		    amp=True
		if  logical_file_name:
		    if amp: add_to_url += "&"
		    add_to_url += "logical_file_name=%s"%logical_file_name
		    amp=True

		if add_to_url:
		    return self.callServer("/outputconfigs?%s" % add_to_url )
		# Default, list all datasets
                return self.callServer("/outputconfigs")

    	def insertAcquisitionEra(self, acqEraObj={}):
                """
                * API to insert An Acquisition Era in DBS 
                * acqEraObj : Acquisition Era object of type {}, with key(s) :-
			* acquisition_era_name : Acquisition Era Name --REQUIRED
                """
                return self.callServer("/acquisitioneras", params = acqEraObj , callmethod='POST' )
		
	def insertProcessingEra(self, procEraObj={}):
                """
                * API to insert A Processing Era in DBS 
                * procEraObj : Processing Era object of type {}
			    * processing_version : Processing Version --REQUIRED
			    * description : Description --REQUIRED
                """
                return self.callServer("/processingeras", params = procEraObj , callmethod='POST' )

        def listDatasets(self, dataset="", parent_dataset="", release_version="", pset_hash="", app_name="", output_module_label="", processing_version="", acquisition_era="",
		run_num="", physics_group_name="", logical_file_name="", primary_ds_name="", primary_ds_type="", data_tier_name="", dataset_access_type="", detail=False):
                """
                * API to list dataset(s) in DBS 
                * dataset : Full dataset (path) of the dataset
		* parent_dataset : Full dataset (path) of the dataset
		* release_version : cmssw version
		* pset_hash : pset hash
		* app_name : Application name (generally it is cmsRun)
		* output_module_label : output_module_label
		* processing_version : Processing Version
		* acquisition_era : Acquisition Era
		* primary_ds_name : Primary Dataset Name
		* primary_ds_type : Primary Dataset Type (Type of data, MC/DATA)
		* data_tier_name : Data Tier 
	        * dataset_access_type : Dataset Access Type ( PRODUCTION, DEPRECATED etc.)
		*
		* You can use ANY combination of these parameters in this API
		* In absence of parameters, all datasets know to DBS instance will be returned
                """
		add_to_url=""
		amp=False
		if dataset: 
		    add_to_url += "dataset=%s"%dataset
		    amp=True
		if parent_dataset: 
		    if amp: add_to_url += "&"
		    add_to_url += "parent_dataset=%s"%parent_dataset
		    amp=True
		if release_version:
		    if amp: add_to_url += "&"
		    add_to_url += "release_version=%s"%release_version
		    amp=True
		if pset_hash:
		    if amp: add_to_url += "&"
		    add_to_url += "pset_hash=%s"%pset_hash
		    amp=True
		if app_name:
		    if amp: add_to_url += "&"
		    add_to_url += "app_name=%s"%app_name
		    amp=True
		if  output_module_label:
		    if amp: add_to_url += "&"
		    add_to_url += "output_module_label=%s"%output_module_label
		    amp=True
		if processing_version:
		    if amp: add_to_url += "&"
		    add_to_url += "processing_version=%s"%processing_version
		    amp=True
		if acquisition_era:
		    add_to_url += "acquisition_era=%s"%acquisition_era
		    amp=True
		if logical_file_name : 
		    if amp: add_to_url += "&"
		    add_to_url += "logical_file_name=%s" % logical_file_name
		    amp=True
		if primary_ds_name : 
		    if amp: add_to_url += "&"
		    add_to_url += "primary_ds_name=%s" % primary_ds_name
		    amp=True
		if data_tier_name: 
		    if amp: add_to_url += "&"
		    add_to_url += "data_tier_name=%s" % data_tier_name
		    amp=True
		if dataset_access_type : 
		    if amp: add_to_url += "&"
		    add_to_url += "dataset_access_type=%s" % dataset_access_type
		    amp=True
		if dataset_access_type : 
		    if amp: add_to_url += "&"
		    add_to_url += "dataset_access_type=%s" % dataset_access_type
		    amp=True
		if detail: 
		    if amp: add_to_url += "&"
		    add_to_url += "detail=%s" % detail
		    amp=True
		if add_to_url:
		    return self.callServer("/datasets?%s" % add_to_url )
		# Default, list all datasets
                return self.callServer("/datasets")
    
	def listDatasetParents(self, dataset):
                """
                * API to list A datasets parents in DBS 
		* dataset : dataset --REQUIRED
                """
                return self.callServer("/datasetparents?dataset=%s" %dataset )

	def listDatasetChildren(self, dataset):
                """
                * API to list A datasets children in DBS 
		* dataset : dataset --REQUIRED
                """
                return self.callServer("/datasetchildren?dataset=%s" %dataset )

        def insertDataset(self, datasetObj={}):
                """
                * API to list A primary dataset in DBS 
                * datasetObj : dataset object of type {}, with key(s) :-
		    *  processed_ds_name : Processed Dataset Name
		    * primary_ds_name : Primary Dataset Name
		    * is_dataset_valid : Is Dataset Valid (1/0)
		    * xtcrosssection : Xtcrosssection
		    * global_tag : Global Tag
		    * output_configs : Output Configs (List of)
			    o app_name : App Name
			    o release_version : Release Version
			    o pset_hash : Pset Hash
			    o output_module_label : Output Module Label 
                """
                return self.callServer("/datasets", params = datasetObj , callmethod='POST' )

	def insertSite(self, siteObj={}):
                """
                * API to insert a site in DBS 
                * siteObj : Site object of type {}, with key(s) :-
		    site_name : Site Name (T1_CMS_FNAL) 
                """
                return self.callServer("/sites", params = siteObj , callmethod='POST' )

        def insertBlock(self, blockObj={}):
                """
                * API to insert a block into DBS 
                * blockObj : block object, with key(s) :-
		        * open_for_writing : Open For Writing (1/0) (Default 1)
			* block_size : Block Size (Default 0)
			* file_count : File Count (Default 0)
			* block_name : Block Name --REQUIRED
			* origin_site_name : Origin Site Name --REQUIRED 
                """
                return self.callServer("/blocks", params = blockObj , callmethod='POST' )

        def listBlocks(self, block_name="", dataset="", logical_file_name="", origin_site_name="", run_num=-1, detail=False):
                """
                * API to list A block in DBS 
                * block_name : name of the block
		* dataset : dataset
		* logical_file_name : Logical File Name
		* origin_site_name : Origin Site Name
		* run_num : Run Number
                """
		add_to_url=""
		amp=False
		if block_name:
		    parts=block_name.split('#')
		    block_name=parts[0]+urllib.quote_plus('#')+parts[1]
		    add_to_url+="block_name=%s" %block_name
		    amp=True
		if dataset:
		    if amp: add_to_url+="&"
		    add_to_url="dataset=%s" %dataset
		    amp=True
		if origin_site_name:
		    if amp: add_to_url+="&"
		    add_to_url += "origin_site_name=%s" % origin_site_name
		if  logical_file_name:
		    if amp: add_to_url += "&"
		    add_to_url += "logical_file_name=%s"%logical_file_name
		    amp=True
		if run_num:
		    if amp: add_to_url += "&"
		    add_to_url += "run_num=%s" %run_num
		    amp=True
		if detail: 
		    if amp: add_to_url += "&"
		    add_to_url += "detail=%s" % detail
		    amp=True

		return self.callServer("/blocks?%s" %add_to_url)

        def listFiles(self, logical_file_name="", dataset="", block="", release_version="", pset_hash="", app_name="", output_module_label="", minrun="", maxrun="", origin_site_name="", lumi_list=[], detail=False):
                """
                * API to list A file in DBS 
                * logical_file_name : logical_file_name of file
		* dataset : dataset
		* block : block name
		* release_version : release version
		* pset_hash
		* app_name
		* output_module_label
		* minrun/maxrun : if you want to look for a run range use these 
				  Use minrun=maxrun for a specific run, say for runNumber 2000 use minrun=2000, maxrun=2000
		* origin_site_name : site where file was created
                """

		add_to_url=""
		amp=False
		if dataset: 
		    add_to_url += "dataset=%s"%dataset
		    amp=True
		if release_version:
		    if amp: add_to_url += "&"
		    add_to_url += "release_version=%s"%release_version
		    amp=True
		if pset_hash:
		    if amp: add_to_url += "&"
		    add_to_url += "pset_hash=%s"%pset_hash
		    amp=True
		if app_name:
		    if amp: add_to_url += "&"
		    add_to_url += "app_name=%s"%app_name
		    amp=True
		if  output_module_label:
		    if amp: add_to_url += "&"
		    add_to_url += "output_module_label=%s"%output_module_label
		    amp=True
		if block :
		    parts=block.split('#')
		    block_name=parts[0]+urllib.quote_plus('#')+parts[1]
		    if amp: add_to_url += "&"
		    add_to_url += "block_name=%s" %block_name	
		    amp=True    
		if logical_file_name : 
		    if amp: add_to_url += "&"
		    add_to_url += "logical_file_name=%s" % logical_file_name
		    amp=True
		if minrun:
		    if amp: add_to_url += "&"
		    add_to_url += "minrun=%s" %minrun
		    amp=True
		if maxrun:
		    if amp: add_to_url += "&"
		    add_to_url += "maxrun=%s" %maxrun
		    amp=True
		if origin_site_name:
		    if amp: add_to_url += "&"
		    add_to_url += "origin_site_name=%s" %origin_site_name
		    amp=True
		if len(lumi_list) > 0 :
		    #FIXME instead of cjson encoding will need to support the list of intervals, like "1-10,12-24" string
		    if amp: add_to_url += "&"
		    add_to_url += "lumi_list=%s" % lumi_list
		if detail: 
		    if amp: add_to_url += "&"
		    add_to_url += "detail=%s" % detail
		    amp=True
		if add_to_url:
		    return self.callServer("/files?%s" % add_to_url )
		else:
		    raise Exception("You must supply parameters to listFiles calls")
		    
	def listFileParents(self, logical_file_name=""):
	    """
	    * API to list file parents
	    * logical_file_name : logical_file_name of file
	    """
	    return self.callServer("/fileparents?logical_file_name=%s" %logical_file_name)

        def listFileChildren(self, logical_file_name=""):
	    """
	    * API to list file children
	    * logical_file_name : logical_file_name of file
	    """
	    return self.callServer("/filechildren?logical_file_name=%s" %logical_file_name)

	def listFileLumis(self, logical_file_name=""):#, block_name):
	    """
	    * API to list Lumi for files
	    * logical_file_name : logical_file_name of file
	    """
	    return self.callServer("/filelumis?logical_file_name=%s" %logical_file_name)

        def insertFiles(self, filesList=[], qInserts=True):
                """
                * API to insert a list of file into DBS in DBS 
                * filesList : list of file objects
		* qInserts : (NEVER use this parameter, unless you are TOLD by DBS Team)
                """

		if qInserts==False: #turn off qInserts
		    return self.callServer("/files?qInserts=%s" % qInserts, params = filesList , callmethod='POST' )    
                return self.callServer("/files", params = filesList , callmethod='POST' )

        def listRuns(self, minrun="", maxrun=""):
                """
                * API to list runs in DBS 
		* minrun: minimum run number	
		* maxrun: maximum run number
		* (minrun, max)	defines the run range
		*
		* If you omit both min/maxrun, then all runs known to DBS will be listed
		* Use minrun=maxrun for a specific run, say for runNumber 2000 use minrun=2000, maxrun=2000
                """
		add_to_url=""
		amp=False
		if minrun:
		    if amp: add_to_url += "&"
		    add_to_url += "minrun=%s" %minrun
		    amp=True
		if maxrun:
		    if amp: add_to_url += "&"
		    add_to_url += "maxrun=%s" %maxrun
		    amp=True
		if add_to_url:
		    return self.callServer("/runs?%s" % add_to_url )
		else:
		    return self.callServer("/runs")

	def listSites(self, block_name="", site_name=""):
                """
                * API to list sites (or for a block) in DBS 
                * block_name : name of the block (The listed site is the ORIGIN_SITE of the block)
		* site_name : name of site
		* Both parameters are optional, and mutually exclusive.
		* In case none provided, DBS returns list of all known sites
		* In case both provided, block_name takes precedence
                """
		add_to_url=""
		amp=False
		if block_name:
		    parts=block_name.split('#')
		    block_name=parts[0]+urllib.quote_plus('#')+parts[1]
		    add_to_url+="block_name=%s" %block_name
		    amp=True
		if site_name:
		    if amp: add_to_url+="&"
		    add_to_url += "site_name=%s"%site_name
		if add_to_url:
		    return self.callServer("/sites?%s" %add_to_url)
		else:	    
		    return self.callServer("/sites")

	def updateFileStatus(self, logical_file_name="", is_file_valid=1):
	    """
	    API to update file status
	    * logical_file_name : logical_file_name --REQUIRED
	    * is_file_valid : valid=1, invalid=0 --REQUIRED
	    """
	    return self.callServer("/files?logical_file_name=%s&is_file_valid=%s" %(logical_file_name, is_file_valid), params={}, callmethod='PUT')

	def updateDatasetType(self, dataset, dataset_access_type):
	    """
	    API to update dataset status
	    * dataset : Dataset --REQUIRED
	    * dataset_access_type : production, deprecated, etc --REQUIRED
	    *
	    """
	    return self.callServer("/datasets?dataset=%s&dataset_access_type=%s" %(dataset, dataset_access_type), params={}, callmethod='PUT')    

	def updateDatasetStatus(self, dataset, is_dataset_valid):
	    """
	    API to update dataset status
	    * dataset : dataset name --REQUIRED
	    * is_dataset_valid : valid=1, invalid=0 --REQUIRED
	    *
	    """
	    return self.callServer("/datasets?dataset=%s&is_dataset_valid=%s" %(dataset, is_dataset_valid), params={}, callmethod='PUT')    
		
	def updateBlockStatus(self, block_name, open_for_writing):
	    """
	    API to update block status
	    * block_name : block name
	    * open_for_writing : open_for_writing=0 (close), open_for_writing=1 (open)
	    """
	    parts=block_name.split('#')
	    block_name=parts[0]+urllib.quote_plus('#')+parts[1]
	    return self.callServer("/blocks?block_name=%s&open_for_writing=%s" %(block_name, open_for_writing), params={}, callmethod='PUT')

	def listDataTypes(self, dataset=""):
	    """
	    API to list data types known to dbs (when no parameter supplied)
	    dataset: If provided, will return data type (of primary dataset) of the dataset
	    """

	    add_to_url=""
	    if dataset:
		add_to_url+="?dataset=%s" %dataset
	    
	    return self.callServer("/datatypes%s" %add_to_url)

        def listDataTiers(self, datatier=""):
	    """
	    API to list data tiers  known to DBS
	    datatier : when supplied, dbs will list details on this tier
	    """

	    add_to_url=""
	    if datatier:
		add_to_url+="?data_tier_name=%s" % datatier
	    
	    return self.callServer("/datatiers%s" %add_to_url)
   
	def listBlockParents(self, block_name=""):
	    """
	    API to list block parents
	    * block_name : name of block whoes parents needs to be found --REQUIRED
	    """
	    parts=block_name.split('#')
	    block_name=parts[0]+urllib.quote_plus('#')+parts[1]
	    return self.callServer("/blockparents?block_name=%s" %block_name)
	   
	def listBlockChildren(self, block_name=""):
	    """
	    API to list block children
	    * block_name : name of block whoes children needs to be found --REQUIRED
	    """
	    parts=block_name.split('#')
	    block_name=parts[0]+urllib.quote_plus('#')+parts[1]
	    return self.callServer("/blockchildren?block_name=%s" %block_name)

	def insertDataTier(self, dataTierObj={}):
            """
            * API to insert A Data Tier in DBS 
            * dataTierObj : Data Tier object of type {}, with kys :-
		    data_tier_name : Data Tier that needs to be inserted
            """
            return self.callServer("/datatiers", params = dataTierObj , callmethod='POST' )

	def migrateSubmit(self, inp):
	    """ Submit a migrate request to migration service"""
	    return self.callServer("/submit", params=inp, callmethod='POST') 

	def migrateStatus(self, migration_request_id="", block_name="", dataset="", user=""):
	    """Check the status of migration request"""
	    amp=False
	    add_to_url=""
	    if  migration_request_id:
		add_to_url+="migration_request_id=%s" %migration_request_id
		amp=True
	    if dataset:
		if amp: add_to_url += "&"
		add_to_url+="dataset=%s" %dataset
		amp=True
	    if block_name:
		parts=block_name.split('#')
		block_name=parts[0]+urllib.quote_plus('#')+parts[1]
		if amp: add_to_url += "&"
		add_to_url += "block_name=%s" %block_name
		amp=True
	    if user:
		if amp: add_to_url += "&"
		add_to_url += "user=%s" %user
	    if add_to_url:
		return self.callServer("/status?%s" % add_to_url )
	    return self.callServer("/status")
	    
if __name__ == "__main__":
	# DBS Service URL
	url="http://cmssrv18.fnal.gov:8585/dbs3"
	#read_proxy="http://cmst0frontier1.cern.ch:3128"
	#read_proxy="http://cmsfrontier1.fnal.gov:3128"
	read_proxy=""
	api = DbsApi(url=url, proxy=read_proxy)
	print api.serverinfo()
	#print api.listPrimaryDatasets()
	#print api.listAcquisitionEras()
	#print api.listProcessingEras()
    
