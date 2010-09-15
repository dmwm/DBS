# 
# $Revision: 1.9 $"
# $Id: dbsClient.py,v 1.9 2009/11/12 22:47:17 afaq Exp $"
# @author anzar
#
import os, sys
import urllib, urllib2
from httplib import HTTPConnection
from StringIO import StringIO
from exceptions import Exception


try:
	# Python 2.6
	import json
except:
	# Prior to 2.6 requires simplejson
	import simplejson as json

class DbsApi:
        def __init__(self, url=""):
                self.url=url

	def callServer(self, urlplus="", params={}):
		"""
        	* callServer
		* API to make HTTP call to the DBS Server
		* url: serevr URL
        	* params: parameters to server, includes api name to be invoked

		"""
		UserID="anzar"	
		headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain/application/x-json",
                                                        "UserID": UserID}

        	#headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain",
        	#                                                "UserID": UserID}

		#print "How to set out the USER ID here ??"
		res='{"FAILED":"TRUE"}'
		try:
			calling=self.url+urlplus
			print calling
			if params == {} :
				data = urllib2.urlopen(calling)
			else:
				params = json.dumps(dict(params))
				req = urllib2.Request(url=calling, headers = header)
				req.get_method = lambda: 'PUT'
				req.add_data(params)
				data = urllib2.urlopen(req)

			res = data.read()
		except urllib2.HTTPError, httperror:
			print httperror
			#HTTPError(req.get_full_url(), code, msg, hdrs, fp)

		except urllib2.URLError, urlerror:
			print urlerror

		return res

		#FIXME: We will always return JSON from DBS, even from POST, PUT, DELETE APIs, make life easy here
		#json_ret=json.loads(res)
		#return json_ret

        def ping(self):
                """
                * API to retrieve DAS interface for DQM Catalog Service
                * getInfo
                """
                return self.callServer("/ping")

	def listPrimaryDatasets(self):
		"""
		* API to list ALL primary datasets in DBS 
		"""
		return self.callServer("/primarydatasets")

        def listPrimaryDataset(self, dataset):
                """
                * API to list A primary dataset in DBS 
		* name : name of the primary dataset
                """
                return self.callServer("/primarydatasets?primarydataset=%s" %dataset )

        def insertPrimaryDataset(self, primaryDSObj={}):
                """
                * API to insert A primary dataset in DBS 
                * primaryDSObj : primary dataset object of type {}
                """
                return self.callServer("/primarydatasets", params = primaryDSObj )

        def listDatasets(self):
                """
                * API to list ALL datasets in DBS
                """
                return self.callServer("/datasets")

        def listDataset(self, dataset):
                """
                * API to list A primary dataset in DBS 
                * Dataset : Full dataset (path) of the dataset
                """
                return self.callServer("/datasets?dataset=%s" % dataset )

        def insertDataset(self, datasetObj={}):
                """
                * API to list A primary dataset in DBS 
                * datasetObj : dataset object of type {}
                """
                return self.callServer("/datasets", params = datasetObj )

        def insertBlock(self, blockObj={}):
                """
                * API to insert a block into DBS 
                * blockObj : block object
                """
                return self.callServer("/blocks", params = blockObj )

        def listBlock(self, block):
                """
                * API to list A block in DBS 
                * name : name of the block
                """
		#return self.callServer("/blocks?block=%s" %block)
		parts=block.split('#')
		black_name=parts[0]+urllib.quote_plus('#')+parts[1]
                return self.callServer("/blocks?block=%s" %black_name )

        def listFile(self, lfn="", dataset="", block=""):
                """
                * API to list A file in DBS 
                * lfn : lfn of file
                """
                if lfn not in (None, "") : return self.callServer("/files?lfn=%s" %lfn)
                if dataset not in (None, "") : return self.callServer("/files?dataset=%s" %dataset)
                if block not in (None, "") : 
			parts=block.split('#')
			black_name=parts[0]+urllib.quote_plus('#')+parts[1]
			return self.callServer("/files?block=%s" %black_name)

        def insertFiles(self, filesList=[]):
                """
                * API to insert a list of file into DBS in DBS 
                * filesList : list of file objects
                """
                return self.callServer("/files", params = filesList )


if __name__ == "__main__":
	# JAVA Service URL
	url="http://cmssrv48.fnal.gov:8989/DBSServlet"
	# API Object	
	#api = DbsApi(url=url)
	# Is service Alive
	#print api.ping()
	# List ALL primary datasets
	#print api.listPrimaryDatasets()
	# List the dataset whoes name is TEST9
	#print api.listPrimaryDataset("TEST9")
	#
	#print "Caling insert primary dataset..."
	#prdsobj = {"PRIMARY_DS_NAME":"ANZAR001", "PRIMARY_DS_TYPE":"test"}
	#api.insertPrimaryDataset(prdsobj)
	
	# Python Service URL
	url="http://cmssrv18.fnal.gov:8585/dbs3"
	api = DbsApi(url=url)
	print api.listPrimaryDatasets()

