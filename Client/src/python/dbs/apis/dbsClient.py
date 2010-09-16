# 
# $Revision: 1.27 $"
# $Id: dbsClient.py,v 1.27 2010/02/19 22:54:05 afaq Exp $"
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
                self.url=url
		self.proxy=proxy
		self.opener =  urllib2.build_opener()

	def callServer(self, urlplus="", params={}):
		"""
        	* callServer
		* API to make HTTP call to the DBS Server
		* url: serevr URL
        	* params: parameters to server, includes api name to be invoked

		"""
		UserID=os.environ['USER']+'@'+socket.gethostname()
		headers =  {"Content-type": "application/json", "Accept": "application/json", "UserID": UserID }
		#headers =  {"Content-type": "application/json", "Accept": "*/*", "UserID": UserID }

		res=""
		try:
			calling=self.url+urlplus
			proxies = {}
			if self.proxy not in (None, ""):
				proxies = { 'http': self.proxy }
			#print calling
			if params == {} :
				#data = urllib.urlopen(calling, proxies=proxies)
				#data = urllib2.urlopen(calling)
				req = urllib2.Request(url=calling, headers = headers)
				data = urllib2.urlopen(req)
			else:
				params = cjson.encode(params)
				req = urllib2.Request(url=calling, data=params, headers = headers)
				req.get_method = lambda: 'POST'
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
	    if type(data)==type("abc"):
		data=json.loads(data)
            if type(data) == type({}) and data.has_key('exception'):
		#print "Service Raised an exception: "+data['exception']
		raise Exception("DBS Server raised an exception: %s" %data['message'])
	    return data
		
        def ping(self):
                """
                * API to retrieve DAS interface for DQM Catalog Service
                * getInfo
                """
                return self.callServer("/ping")

	def listPrimaryDatasets(self, dataset=""):
		"""
		* API to list ALL primary datasets in DBS 
		"""
		if dataset:
		    return self.callServer("/primarydatasets?primary_ds_name=%s" %dataset )
		return self.callServer("/primarydatasets")

        def insertPrimaryDataset(self, primaryDSObj={}):
                """
                * API to insert A primary dataset in DBS 
                * primaryDSObj : primary dataset object of type {}
                """
                return self.callServer("/primarydatasets", params = primaryDSObj )

	def insertOutputConfig(self, outputConfigObj={}):
                """
                * API to insert An OutputConfig in DBS 
                * outputConfigObj : Output Config object of type {}
                """
                return self.callServer("/outputconfigs", params = outputConfigObj )

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
                * acqEraObj : Acquisition Era object of type {}
                """
                return self.callServer("/acquisitionras", params = acqEraObj )
		
	def insertProcessingEra(self, procEraObj={}):
                """
                * API to insert A Processing Era in DBS 
                * procEraObj : Processing Era object of type {}
                """
                return self.callServer("/processingeras", params = procEraObj )

        def listDatasets(self, dataset="", parent_dataset="", release_version="", pset_hash="", app_name="", output_module_label=""):
                """
                * API to list dataset(s) in DBS 
                * dataset : Full dataset (path) of the dataset
		* parent_dataset : Full dataset (path) of the dataset
		* release_version : cmssw version
		* pset_hash : pset hash
		* app_name : Application name (generally it is cmsRun)
		* output_module_label : output_module_label
		*
		* You can use ANY combination of these parameters in this API
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
		
		if add_to_url:
		    return self.callServer("/datasets?%s" % add_to_url )
		# Default, list all datasets
                return self.callServer("/datasets")
    
	def listDatasetParents(self, dataset):
                """
                * API to list A datasets parents in DBS 
		* dataset : dataset
                """
                return self.callServer("/datasetparents?dataset=%s" %dataset )

        def insertDataset(self, datasetObj={}):
                """
                * API to list A primary dataset in DBS 
                * datasetObj : dataset object of type {}
                """
                return self.callServer("/datasets", params = datasetObj )

	def insertSite(self, siteObj={}):
                """
                * API to insert a site in DBS 
                * siteObj : Site object of type {}
                """
                return self.callServer("/sites", params = siteObj )

        def insertBlock(self, blockObj={}):
                """
                * API to insert a block into DBS 
                * blockObj : block object
                """
                return self.callServer("/blocks", params = blockObj )

        def listBlocks(self, block_name="", dataset="", site_name=""):
                """
                * API to list A block in DBS 
                * name : name of the block
                """
		url_param=""
		addAnd=False
		if block_name:
		    parts=block_name.split('#')
		    block_name=parts[0]+urllib.quote_plus('#')+parts[1]
		    url_param+="block_name=%s" %block_name
		    addAnd=True
		if dataset:
		    if addAnd: url_param+="&"
		    url_param="dataset=%s" %dataset
		    addAnd=True
		if site_name:
		    if addAnd: url_param+="&"
		    url_param += "site_name=%s"%site_name
		    
		return self.callServer("/blocks?%s" %url_param)

        def listFiles(self, lfn="", dataset="", block="", release_version="", pset_hash="", app_name="", output_module_label=""):
                """
                * API to list A file in DBS 
                * lfn : lfn of file
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
		if lfn : 
		    if amp: add_to_url += "&"
		    add_to_url += "logical_file_name=%s" %lfn
		    amp=True
		    
		if add_to_url:
		    return self.callServer("/files?%s" % add_to_url )
		else:
		    raise Exception("You must supply parameters to listFiles calls")
		    
	def listFileParents(self, lfn=""):
	    """
	    * API to list file parents
	    * lfn : lfn of file
	    """
	    return self.callServer("/fileparents?logical_file_name=%s" %lfn)

	def listFileLumis(self, lfn=""):#, block_name):
	    """
	    * API to list Lumi for files
	    * lfn : lfn of file
	    """
	    return self.callServer("/filelumis?logical_file_name=%s" %lfn)

        def insertFiles(self, filesList=[]):
                """
                * API to insert a list of file into DBS in DBS 
                * filesList : list of file objects
                """
                return self.callServer("/files", params = filesList )


if __name__ == "__main__":
	# DBS Service URL
	url="http://cmssrv18.fnal.gov:8586/dbs3"
	read_proxy="http://cmst0frontier1.cern.ch:3128"
	read_proxy="http://cmsfrontier1.fnal.gov:3128"
	read_proxy=""
	api = DbsApi(url=url, proxy=read_proxy)
	print api.listPrimaryDatasets()

