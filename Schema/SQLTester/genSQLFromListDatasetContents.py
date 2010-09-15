#!/usr/bin/env python
#
import sys
from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsException import *
from DBSAPI.dbsApiException import *
from DBSAPI.dbsOptions import DbsOptionParser
#
import xml.sax, xml.sax.handler
#
try:
  optManager  = DbsOptionParser()
  (opts,args) = optManager.getOpt()
  api = DbsApi(opts.__dict__)
  datasets=[	
		#"/Cosmics/Commissioning08-v1/RAW", 
		#"/TTbar/Summer09-MC_31X_V3-v1/GEN-SIM-RAW", 
		"/TTbar/Summer09-MC_31X_V3-v1/GEN-SIM-RECO", 
		"/BeamHalo/Summer09-STARTUP31X_V7_StreamMuAlBeamHaloOverlaps-v1/ALCARECO", 
		"/InclusiveMu15/Summer09-MC_31X_V3_7TeV-v1/GEN-SIM-RAW", 
		"/Cosmics/CMSSW_3_2_7-CRAFT09_R_V4_CosmicsSeq-v1/RECO", 
		"/Wmunu/Summer09-MC_31X_V3_7TeV_SD_L1_L2_Mu-v1/GEN-SIM-RECO", 
		"/Wmunu/Summer09-MC_31X_V3_7TeV_SD_Mu9-v1/GEN-SIM-RECO" 
		]
  #datasets=["/TTbar/Summer09-MC_31X_V3-v1/GEN-SIM-RAW"]
  for dataset in datasets :
    blocks=api.listBlocks(dataset)
    for ablock in blocks:
	data=api.listDatasetContents(dataset, ablock["Name"])
  	#print data
	print "-- SQL Statements for Dataset : %s and Block : %s " % (dataset, ablock["Name"])
  	class Handler (xml.sax.handler.ContentHandler):

		def __init__(self):
			self.sqls={}		
			self.sqls['paths']=[]
			self.sqls['storage_element']=[]
			self.sqls['block_storage_elements']=[]
			self.sqls['file']=[]
                	self.sqls['app_version']=[]
                	self.sqls['app_executable_name']=[]
                	self.sqls['ps_hash']=[]
                	self.sqls['process_configurations']=[]
			self.sqls['processed_dataset_algorithm']=[]
			self.sqls['file_algorithm']=[]
			self.sqls['run']=[]
			self.sqls['lfn']=[]
			self.sqls['file_lumi_section']=[]
			self.already_run=[]

			self.primary_dataset=''
			self.processed_dataset=''
			self.creation_date=''
			self.created_by=''
			self.block_name=''
			self.dataset=''
			self.id=0

        	def startElement(self, name, attrs):
			if name == 'primary_dataset':
				self.primary_dataset=attrs.get('primary_name')
				self.creation_date=attrs.get('creation_date')
				self.created_by=attrs.get('created_by')
				self.sqls['primary']="\nINSERT INTO PRIMARY_DATASETS( PRIMARY_DS_NAME, PRIMARY_DS_TYPE_ID, CREATION_DATE, CREATE_BY) VALUES ('%s', (SELECT PRIMARY_DS_TYPE_ID FROM PRIMARY_DS_TYPES WHERE PRIMARY_DS_TYPE = '%s'), '%s', '%s');" % ( attrs.get('primary_name'), attrs.get('type'), attrs.get('creation_date'), attrs.get('created_by') ) 

			if name == 'processed_dataset':
				self.processed_dataset=attrs.get('processed_datatset_name')
				self.sqls['processed']="\nINSERT INTO PROCESSED_DATASETS( PROCESSED_DS_NAME) VALUES ('%s');" % (attrs.get('processed_datatset_name'))

			if name == 'path':
				self.data_tier = str(attrs.get('dataset_path')).split('/')[3]

				self.sqls['data_tier'] = "\nINSERT INTO DATA_TIERS( DATA_TIER_NAME, CREATION_DATE, CREATE_BY) VALUES ('%s', '%s', '%s');" % ( self.data_tier, self.creation_date, self.created_by)

				self.path=attrs.get('dataset_path')
				self.sqls['paths'].append("\nINSERT INTO DATASETS ( DATASET, IS_DATASET_VALID, PRIMARY_DS_ID, PROCESSED_DS_ID, DATA_TIER_ID, DATASET_TYPE_ID, ACQUISITION_ERA_ID, PROCESSING_ERA_ID, PHYSICS_GROUP_ID, XTCROSSSECTION, GLOBAL_TAG, CREATION_DATE, CREATE_BY, LAST_MODIFICATION_DATE, LAST_MODIFIED_BY) VALUES ('%s', '%s', (SELECT PRIMARY_DS_ID FROM PRIMARY_DATASETS WHERE PRIMARY_DS_NAME = '%s'), (SELECT PROCESSED_DS_ID FROM PROCESSED_DATASETS WHERE PROCESSED_DS_NAME = '%s'), (SELECT DATA_TIER_ID FROM DATA_TIERS WHERE DATA_TIER_NAME = '%s'), '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');" % ( attrs.get('dataset_path'), '1', self.primary_dataset, self.processed_dataset, self.data_tier, '1', '1', '1', '1', '123', 'gtag', self.creation_date, self.created_by, self.creation_date, 'anzar') )

			if name == 'block':
				self.block_name=attrs.get('name')
				#self.sqls['block']="\nINSERT INTO BLOCKS( BLOCK_NAME, DATASET_ID, OPEN_FOR_WRITING, ORIGIN_SITE, BLOCK_SIZE, FILE_COUNT, CREATION_DATE, CREATE_BY, LAST_MODIFICATION_DATE, LAST_MODIFIED_BY) VALUES ('%s', (SELECT DATASET_ID FROM DATASETS WHERE DATASET = '%s'), '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');" % ( attrs.get('name'), attrs.get('path'), '1', '1', attrs.get('size'), attrs.get('number_of_files'), attrs.get('creation_date'), attrs.get('created_by'), attrs.get('last_modification_date'), attrs.get('last_modified_by'))
				self.sqls['block']="\nINSERT INTO BLOCKS( BLOCK_NAME, DATASET_ID, OPEN_FOR_WRITING, BLOCK_SIZE, FILE_COUNT, CREATION_DATE, CREATE_BY, LAST_MODIFICATION_DATE, LAST_MODIFIED_BY) VALUES ('%s', (SELECT DATASET_ID FROM DATASETS WHERE DATASET = '%s'), '%s', '%s', '%s', '%s', '%s', '%s', '%s');" % ( attrs.get('name'), attrs.get('path'), '1', attrs.get('size'), attrs.get('number_of_files'), attrs.get('creation_date'), attrs.get('created_by'), attrs.get('last_modification_date'), attrs.get('last_modified_by'))

			if name == 'storage_element':
				self.sqls['storage_element'].append("\nINSERT INTO STORAGE_ELEMENTS( SE_NAME) VALUES ('%s');" % ( attrs.get('storage_element_name')))
				self.sqls['block_storage_elements'].append( "\nINSERT INTO BLOCK_STORAGE_ELEMENTS(SE_ID, BLOCK_ID) VALUES (  (SELECT SE_ID FROM STORAGE_ELEMENTS WHERE SE_NAME='%s'), (SELECT DISTINCT BLOCK_ID FROM BLOCKS WHERE BLOCK_NAME='%s') );" % ( attrs.get('storage_element_name'), self.block_name ) )

			if name == 'file':
				self.sqls['lfn'].append(attrs.get('lfn'))
				self.sqls['file'].append( "\nINSERT INTO FILES( LOGICAL_FILE_NAME, IS_FILE_VALID, DATASET_ID, BLOCK_ID, FILE_TYPE_ID, CHECK_SUM, EVENT_COUNT, FILE_SIZE, ADLER32, MD5, AUTO_CROSS_SECTION, CREATION_DATE, CREATE_BY, LAST_MODIFICATION_DATE, LAST_MODIFIED_BY) VALUES ('%s', '%s', (SELECT DATASET_ID FROM DATASETS WHerE DATASET='%s'), (SELECT BLOCK_ID FROM BLOCKS WHERE BLOCK_NAME='%s'), '%s','%s', '%s', '%s', '%s','%s', '%s', '%s', '%s', '%s', '%s');" %( attrs.get('lfn'), '1', self.path, self.block_name, '1', attrs.get('checksum'), attrs.get('number_of_events'), attrs.get('size'), attrs.get('adler32'), attrs.get('md5'), 1, attrs.get('creation_date'), attrs.get('created_by'), attrs.get('last_modification_date'), attrs.get('last_modified_by') ) ) 
				#self.sqls['file'].append( "\nINSERT INTO FILES( LOGICAL_FILE_NAME, IS_FILE_VALID, DATASET_ID, BLOCK_ID, FILE_TYPE_ID, CHECK_SUM, EVENT_COUNT, FILE_SIZE, BRANCH_HASH_ID, ADLER32, MD5, AUTO_CROSS_SECTION, CREATION_DATE, CREATE_BY, LAST_MODIFICATION_DATE, LAST_MODIFIED_BY) VALUES ('%s', '%s', (SELECT DATASET_ID FROM DATASETS WHerE DATASET='%s'), (SELECT BLOCK_ID FROM BLOCKS WHERE BLOCK_NAME='%s'), '%s','%s', '%s', '%s', '%s', '%s','%s', '%s', '%s', '%s', '%s', '%s');" %( attrs.get('lfn'), '1', self.path, self.block_name, '1', attrs.get('checksum'), attrs.get('number_of_events'), attrs.get('size'), '1', attrs.get('adler32'), attrs.get('md5'), attrs.get('auto_cross_section'), attrs.get('creation_date'), attrs.get('created_by'), attrs.get('last_modification_date'), attrs.get('last_modified_by') ) ) 

			if name == 'algorithm':
				self.sqls['app_version'].append("\nINSERT INTO RELEASE_VERSIONS (VERSION) VALUES ( '%s' ); " % attrs.get('app_version') )
				self.sqls['app_executable_name'].append("\nINSERT INTO APPLICATION_EXECUTABLES (APP_NAME) VALUES ( '%s' );" %attrs.get('app_executable_name') )
				self.sqls['ps_hash'].append("\nINSERT INTO PARAMETER_SET_HASHES(HASH, NAME) VALUES ( '%s', '%s' );" % ('NO_PSET_HASH', 'NO_PSET_HASH') )
				self.sqls['process_configurations'].append( "\nINSERT INTO OUTPUT_MODULE_CONFIGS(APP_EXEC_ID, RELEASE_VERSION_ID, PARAMETER_SET_HASH_ID, OUTPUT_MODULE_LABEL, CREATION_DATE, CREATE_BY) VALUES ( (SELECT APP_EXEC_ID FROM APPLICATION_EXECUTABLES WHERE APP_NAME='%s'), (SELECT RELEASE_VERSION_ID FROM RELEASE_VERSIONS WHERE VERSION='%s'), (SELECT PARAMETER_SET_HASH_ID FROM PARAMETER_SET_HASHES WHERE NAME='NO_PSET_HASH'), '%s', '%s', '%s');" % ( attrs.get('app_executable_name'), attrs.get('app_version'), 'NO_OUTPUT_MODULE_LABEL', self.creation_date , self.created_by ) )

			if name == 'processed_dataset_algorithm':
				self.sqls['processed_dataset_algorithm'].append("\nINSERT INTO DATASET_OUTPUT_MOD_CONFIGS(DATASET_ID, DS_OUTPUT_MOD_CONF_ID) VALUES ((SELECT DATASET_ID FROM DATASETS WHERE DATASET='%s'), (SELECT OUTPUT_MOD_CONFIG_ID FROM OUTPUT_MODULE_CONFIGS WHERE APP_EXEC_ID=(SELECT APP_EXEC_ID FROM APPLICATION_EXECUTABLES WHERE APP_NAME='%s') AND RELEASE_VERSION_ID=(SELECT RELEASE_VERSION_ID FROM RELEASE_VERSIONS WHERE VERSION='%s') AND PARAMETER_SET_HASH_ID=(SELECT PARAMETER_SET_HASH_ID FROM PARAMETER_SET_HASHES WHERE NAME='NO_PSET_HASH')) );" % (self.path, attrs.get('app_executable_name'), attrs.get('app_version')) )


			if name == 'file_algorithm':
				self.sqls['file_algorithm'].append("\nINSERT INTO FILE_OUTPUT_MOD_CONFIGS(FILE_ID, OUTPUT_MOD_CONFIG_ID) VALUES (  (SELECT FILE_ID FROM FILES WHERE LOGICAL_FILE_NAME='__FILE_LFN__'), (SELECT OUTPUT_MOD_CONFIG_ID FROM OUTPUT_MODULE_CONFIGS WHERE APP_EXEC_ID=(SELECT APP_EXEC_ID FROM APPLICATION_EXECUTABLES WHERE APP_NAME='%s') AND RELEASE_VERSION_ID=(SELECT RELEASE_VERSION_ID FROM RELEASE_VERSIONS WHERE VERSION='%s') AND PARAMETER_SET_HASH_ID=(SELECT PARAMETER_SET_HASH_ID FROM PARAMETER_SET_HASHES WHERE NAME='NO_PSET_HASH')) );" % (attrs.get('app_executable_name'), attrs.get('app_version') ) )

			if name == 'file_lumi_section':
				self.sqls['file_lumi_section'].append("\nINSERT INTO FILE_LUMIS(RUN_NUM, LUMI_SECTION_NUM, FILE_ID) VALUES ('%s', '%s', (SELECT FILE_ID FROM FILES WHERE LOGICAL_FILE_NAME='%s') );" % ( attrs.get('run_number'), attrs.get('lumi_section_number'), '__FILE_LFN__' ) ) 

			if name =='run':
				if attrs.get('run_number') not in self.already_run:
					self.sqls['run'].append("\nINSERT INTO DATASET_RUNS(DATASET_ID, RUN_NUMBER, COMPLETE, LUMI_SECTION_COUNT, CREATION_DATE, CREATE_BY) VALUES ((SELECT DATASET_ID FROM DATASETS WHERE DATASET= '%s'), '%s','%s', '%s', '%s', '%s');" % ( self.path, attrs.get('run_number'), '1', attrs.get('number_of_lumi_sections'), self.creation_date , self.created_by ) )
					self.already_run.append(attrs.get('run_number'))

		def endElement(self, name) :
			if name == 'dbs':
				#print self.sqls
				print self.sqls['primary']
				print self.sqls['processed']
				print self.sqls['data_tier']
				for x in self.sqls['paths'] : print x
				print self.sqls['block'] 
				for x in self.sqls['storage_element'] : print x			
				for x in self.sqls['block_storage_elements'] : print x
				for x in self.sqls['app_version'] : print x
				for x in self.sqls['app_executable_name']: print x
				for x in self.sqls['ps_hash'] : print x
				for x in self.sqls['process_configurations'] : print x
				for x in self.sqls['processed_dataset_algorithm'] : print x
				for x in self.sqls['file'] : print x
				for x in self.sqls['lfn'] : 
					for y in self.sqls['file_algorithm'] : y.replace('__FILE_LFN__', x)
					for y in self.sqls['file_lumi_section'] : y.replace('__FILE_LFN__', x)	
				for x in self.sqls['run'] : print x
				print "\n\n"

  	xml.sax.parseString (data, Handler ())

except DbsApiException, ex:
  print "Caught API Exception %s: %s "  % (ex.getClassName(), ex.getErrorMessage() )
  if ex.getErrorCode() not in (None, ""):
    print "DBS Exception Error Code: ", ex.getErrorCode()


