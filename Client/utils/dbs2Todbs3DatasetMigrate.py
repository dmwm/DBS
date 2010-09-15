#!/usr/bin/env python
#
import sys

#DBS-2 imports
from DBSAPI.dbsApi import DbsApi as Dbs2Api
from DBSAPI.dbsException import *
from DBSAPI.dbsApiException import *
from DBSAPI.dbsOptions import DbsOptionParser

#DBS-3 imports
from dbs.apis.dbsClient import *

#
import xml.sax, xml.sax.handler
#
try:
  optManager  = DbsOptionParser()
  (opts,args) = optManager.getOpt()
  api = Dbs2Api(opts.__dict__)
  datasets=[	
		#"/Cosmics/Commissioning08-v1/RAW", 
		#"/TTbar/Summer09-MC_31X_V3-v1/GEN-SIM-RAW", 
		"/TTbar/Summer09-MC_31X_V3-v1/GEN-SIM-RECO", 
		"/BeamHalo/Summer09-STARTUP31X_V7_StreamMuAlBeamHaloOverlaps-v1/ALCARECO", 
		"/InclusiveMu15/Summer09-MC_31X_V3_7TeV-v1/GEN-SIM-RAW", 
		"/Cosmics/CMSSW_3_2_7-CRAFT09_R_V4_CosmicsSeq-v1/RECO", 
		"/Wmunu/Summer09-MC_31X_V3_7TeV_SD_L1_L2_Mu-v1/GEN-SIM-RECO", 
		"/Wmunu/Summer09-MC_31X_V3_7TeV_SD_Mu9-v1/GEN-SIM-RECO",
		"/ZeeJet_Pt230to300/Summer09-MC_31X_V3_7TeV-v1/GEN-SIM-RAW",
		"/TkCosmics38T/Summer09-STARTUP31X_V3_SuperPointing-v1/RAW-RECO"
		]
  datasets=["/TkCosmics38T/Summer09-STARTUP31X_V3_SuperPointing-v1/RAW-RECO"]

  for dataset in datasets :
    blocks=api.listBlocks(dataset)
    for ablock in blocks:
        # Collect information here			
	data=api.listDatasetContents(dataset, ablock["Name"])
  	#print data
	print "Processing Dataset : %s and Block : %s " % (dataset, ablock["Name"])
  	class Handler (xml.sax.handler.ContentHandler):

		def __init__(self):
			self.primary_dataset=''
			self.processed_dataset=''
			self.creation_date=''
			self.created_by=''
			self.block_name=''
			self.dataset=''
        		self.prdsobj={}

        	def startElement(self, name, attrs):
			if name == 'primary_dataset':
				self.primary_dataset=attrs.get('primary_name')
				self.prdsobj = { "PRIMARY_DS_NAME" : str(self.primary_dataset), "PRIMARY_DS_TYPE": "test" }
			if name == 'processed_dataset':
				self.dataset={"IS_DATASET_VALID": 1 , "PRIMARY_DS_NAME": self.primary_dataset, "PRIMARY_DS_TYPE": "test", "DATASET_TYPE":"PRODUCTION",
						"GLOBAL_TAG": attrs.get('global_tag'),"XTCROSSSECTION":123,"PHYSICS_GROUP_NAME": "Tracker", 
						"PROCESSING_VERSION" : "1",
						"PROCESSED_DATASET_NAME": attrs.get('processed_datatset_name'), "ACQUISITION_ERA_NAME" : attrs.get('acquisition_era') }
				self.processed_dataset=attrs.get('processed_datatset_name')

			if name == 'path':
				self.data_tier = str(attrs.get('dataset_path')).split('/')[3]
				self.path=attrs.get('dataset_path')
				self.dataset["DATA_TIER_NAME"]=self.data_tier 
				self.dataset["DATASET"]=self.path

			"""
			if name == 'block':
				print attrs
                                return
				self.block_name=attrs.get('name')
				attrs.get('path')
				attrs.get('size')
				attrs.get('number_of_files')
				attrs.get('creation_date')
				attrs.get('created_by')
				attrs.get('last_modification_date')
				attrs.get('last_modified_by')	

			if name == 'storage_element':
				print attrs
                                return
				attrs.get('storage_element_name')

			if name == 'file':
				print attrs
                                return
				self.sqls['lfn'].append(attrs.get('lfn'))
				attrs.get('checksum')
				attrs.get('number_of_events')
				attrs.get('size')
				attrs.get('adler32')
				attrs.get('md5')
				attrs.get('creation_date')
				attrs.get('created_by')
				attrs.get('last_modification_date')
				attrs.get('last_modified_by')

			if name == 'algorithm':
				print attrs
                                return
				attrs.get('app_version')
				attrs.get('app_executable_name')
				'NO_PSET_HASH'

			if name == 'processed_dataset_algorithm':
				pass

			if name == 'file_algorithm':
				pass

			if name == 'file_lumi_section':
				print attrs
                                return
				attrs.get('run_number')
				attrs.get('lumi_section_number')

			if name =='run':
				attrs.get('run_number') 
				pass
			"""
	
		def endElement(self, name) :
                        if name == 'dbs':	
				print "fin"
				# Lets populate this in DBS
				# Some calls may be redundant, who cares !
        			# DBS-3 Service URL
        			url="http://cmssrv48.fnal.gov:8989/DBSServlet"
        			# API Object    
        			dbs3api = DbsApi(url=url)
        			# Is service Alive
        			print dbs3api.ping()
        			print self.prdsobj
        			print dbs3api.insertPrimaryDataset(self.prdsobj)
				print self.dataset
        			print dbs3api.insertDataset(self.dataset)

  	xml.sax.parseString (data, Handler ())
	break

except DbsApiException, ex:
  print "Caught API Exception %s: %s "  % (ex.getClassName(), ex.getErrorMessage() )
  if ex.getErrorCode() not in (None, ""):
    print "DBS Exception Error Code: ", ex.getErrorCode()


