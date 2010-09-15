# 
# $Revision: 1.3 $"
# $Id: dbsClient.py,v 1.3 2009/11/04 22:35:00 afaq Exp $"
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

		calling=self.url+urlplus
		#print calling
		if params == {} :
			data = urllib2.urlopen(calling)
		else:
			params = json.dumps(dict(params))
			req = urllib2.Request(url=calling)
			req.add_data(params)
			data = urllib2.urlopen(req)

		res = data.read()
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
		return self.callServer("/PrimaryDatasets/")

        def listPrimaryDataset(self, name):
                """
                * API to list A primary dataset in DBS 
		* name : name of the primary dataset
                """
                return self.callServer("/PrimaryDatasets/%s" %name )

        def insertPrimaryDataset(self, primaryDSObj={}):
                """
                * API to insert A primary dataset in DBS 
                * primaryDSObj : primary dataset object of type {}
                """
                return self.callServer("/PrimaryDatasets/", params = primaryDSObj )

        def listDatasets(self):
                """
                * API to list ALL datasets in DBS
                """
                return self.callServer("/Datasets/")

        def listDataset(self, dataset):
                """
                * API to list A primary dataset in DBS 
                * Dataset : Full dataset (path) of the dataset
                """
                return self.callServer("/Datasets/%s" % dataset[1:] )

        def insertDataset(self, datasetObj={}):
                """
                * API to list A primary dataset in DBS 
                * datasetObj : dataset object of type {}
                """
                return self.callServer("/Datasets/", params = datasetObj )



if __name__ == "__main__":
	# Service URL
	url="http://cmssrv48.fnal.gov:8989/DBSServlet"
	# API Object	
	api = DbsApi(url=url)
	# Is service Alive
	print api.ping()
	# List ALL primary datasets
	#print api.listPrimaryDatasets()
	# List the dataset whoes name is TEST9
	#print api.listPrimaryDataset("TEST9")
	#
	#print "Caling insert primary dataset..."
	#prdsobj = {"PRIMARY_DS_NAME":"ANZAR001", "PRIMARY_DS_TYPE":"test"}
	#api.insertPrimaryDataset(prdsobj)


