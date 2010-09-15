#!/usr/bin/env python
#
import sys
import time

#DBS-2 imports
from DBSAPI.dbsApi import DbsApi as Dbs2Api
from DBSAPI.dbsException import *
from DBSAPI.dbsApiException import *
from DBSAPI.dbsOptions import DbsOptionParser

from exceptions import Exception

#DBS-3 imports
from dbs.apis.dbsClient import *

#
import xml.sax, xml.sax.handler
#
#print sys.argv
if len(sys.argv) < 2: 
	print "Usage: python %s <url> <dataset>" %sys.argv[0]
	sys.exit(1)

#url="http://vocms09.cern.ch:8585/dbs3"
url=sys.argv[1]
# DBS3 service 
dbs3api = DbsApi(url=url)
dataset=sys.argv[2]
#primary/dataset are inserted only Once
abc="true"

try:
  optManager  = DbsOptionParser()
  (opts,args) = optManager.getOpt()
  api = Dbs2Api(opts.__dict__)
  datasets=[dataset]
  block_time_lst=[]

  for dataset in datasets :
    blocks=api.listBlocks(dataset)
    for ablock in blocks:
	block_time={}
        # Collect information here	
	# Check if XML file already exists in loacl disk, use that
	#	
	blockName=ablock["Name"]
	fileName = blockName.replace('/', '_').replace('#', '_') + ".xml"
	if os.path.exists(fileName):
		data = open(fileName, "r").read()
	else:	
		data=api.listDatasetContents(dataset, ablock["Name"])
		fp=open(fileName, "w")
		fp.write(data)
		fp.close()
  	#print data
	#print "Processing Dataset : %s and Block : %s " % (dataset, blockName)
  	class Handler (xml.sax.handler.ContentHandler):

		def __init__(self):
			self.primary_dataset=''
			self.processed_dataset=''
			self.creation_date=''
			self.created_by=''
			self.block_name=''
			self.dataset=''
        		self.prdsobj={}
			self.block={}
			self.files=[]
			self.currfile={}
			self.currfilelumis=[]
			self.currfileparents=[]

        	def startElement(self, name, attrs):
			if name == 'primary_dataset':
				self.primary_dataset=attrs.get('primary_name')
				self.prdsobj = { "PRIMARY_DS_NAME" : str(self.primary_dataset), "PRIMARY_DS_TYPE": "test" }
			if name == 'processed_dataset':
				self.dataset=	{
						"IS_DATASET_VALID": 1 , "PRIMARY_DS_NAME": self.primary_dataset, "PRIMARY_DS_TYPE": "test", "DATASET_TYPE":"PRODUCTION",
						"GLOBAL_TAG": attrs.get('global_tag'),"XTCROSSSECTION":123,"PHYSICS_GROUP_NAME": "Tracker", 
						"PROCESSING_VERSION" : "1",
						"PROCESSED_DATASET_NAME": attrs.get('processed_datatset_name'), "ACQUISITION_ERA_NAME" : attrs.get('acquisition_era') 
						}
					
				self.processed_dataset=attrs.get('processed_datatset_name')

			if name == 'path':
				self.data_tier = str(attrs.get('dataset_path')).split('/')[3]
				self.path=attrs.get('dataset_path')
				self.dataset["DATA_TIER_NAME"]=self.data_tier 
				self.dataset["DATASET"]=self.path

			if name == 'block':
				self.block  = {
						"BLOCK_NAME":attrs.get('name'), "OPEN_FOR_WRITING":1,"BLOCK_SIZE": attrs.get('size'), 
						"FILE_COUNT":attrs.get('number_of_files'), "CREATION_DATE":attrs.get('creation_date'), 
						"CREATE_BY":attrs.get('created_by'), "LAST_MODIFICATION_DATE":attrs.get('last_modification_date'), 
						"LAST_MODIFIED_BY":attrs.get('last_modified_by')
						}
				self.block_name=attrs.get('name')

			if name == 'storage_element':
				self.block["ORIGIN_SITE"]=attrs.get('storage_element_name')

			if name == 'file':
				self.currfile={
					"LOGICAL_FILE_NAME":attrs.get('lfn'), "IS_FILE_VALID": 1, "DATASET": self.path, "BLOCK" : self.block_name,
					"FILE_TYPE": "EDM",
					"CHECK_SUM": attrs.get('checksum'), "EVENT_COUNT": attrs.get('number_of_events'), "FILE_SIZE": attrs.get('size'), 
					"ADLER32": attrs.get('adler32'), "MD5": attrs.get('md5'), "AUTO_CROSS_SECTION": 0.0, 
					"CREATE_BY":attrs.get('created_by'), "LAST_MODIFICATION_DATE":attrs.get('last_modification_date'),
                                        "LAST_MODIFIED_BY":attrs.get('last_modified_by')
				     }

			if name == 'file_lumi_section':
				filelumi={
						"RUN_NUM":attrs.get('run_number'),
						"LUMI_SECTION_NUM":attrs.get('lumi_section_number')
					}
				self.currfilelumis.append(filelumi)
			
			if name == 'file_parent':
				fileparent = {
						"FILE_PARENT_LFN":attrs.get('lfn')
					}
				self.currfileparents.append(fileparent)

			"""
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

			if name =='run':
				attrs.get('run_number') 
				pass
			"""
	
		def endElement(self, name) :
			
			if name == 'file' : 
				self.currfile["FILE_LUMI_LIST"]=self.currfilelumis
				self.currfile["FILE_PARENT_LIST"]=self.currfileparents
				self.currfilelumis=[]
				self.currfileparents=[]
				self.files.append(self.currfile)

                        if name == 'dbs':
				try :
					# Lets populate this in DBS
        				# API Object  
					if abc == "true" :
        					#print self.prdsobj
        					dbs3api.insertPrimaryDataset(self.prdsobj)

						##print self.dataset
        					dbs3api.insertDataset(self.dataset)
						again="no"
					#print self.block
					dbs3api.insertBlock(self.block)
					start_time=time.time()
					#for file in self.files:
					#	print file
					dbs3api.insertFiles({"files" : self.files})
					end_time=time.time()
					block_time['TimeSpent']=end_time-start_time
					block_time['block_weight']=long(len(self.files))
					block_time['file_count']=long(len(self.files))
					block_time['file_lumi_section_count']=0
					block_time['file_parent_count']=0
					for file in self.files:
						if file.has_key('FILE_LUMI_LIST'):
							block_time['block_weight']+=long(len(file['FILE_LUMI_LIST']))
							block_time['file_lumi_section_count']+=long(len(file['FILE_LUMI_LIST']))
						if file.has_key('FILE_PARENT_LIST'):
							block_time['block_weight']+=long(len(file['FILE_PARENT_LIST']))
							block_time['file_parent_count']+=long(len(file['FILE_PARENT_LIST']))
					block_time_lst.append(block_time)
					#print "fin"
				except Exception, ex:
					print ex
				
  	xml.sax.parseString (data, Handler ())
  print "dataset: %s" %dataset
  print "-------------------------------------------------------------------------------------------------\n\n"
  print "url: %s" % url
  print "-------------------------------------------------------------------------------------------------\n\n"
  print "RAW DATA: %s " % str(block_time_lst)
  print "\n"
  total_t=0.0
  total_b=0.0
  for item in block_time_lst:
	print "Time Spent : %s (seconds) while Block Weightage is : %s [files: %s, avg lumis_per_file: %s, avg parent_per_file: %s]" \
				% ( str(item['TimeSpent']), str(item['block_weight']), item['file_count'], \
						str( item['file_lumi_section_count']/item['file_count'] ), str(item['file_parent_count']/item['file_count'] ) )
	total_t+=item['TimeSpent']
	total_b+=item['block_weight']
  print "-------------------------------------------------------------------------------------------------\n\n"
  print "Total time spent: %s (seconds) for total block weightage of: %s " %( str(total_t), str(total_b) )
  print "-------------------------------------------------------------------------------------------------\n\n"
except DbsApiException, ex:
  print "Caught API Exception %s: %s "  % (ex.getClassName(), ex.getErrorMessage() )
  if ex.getErrorCode() not in (None, ""):
    print "DBS Exception Error Code: ", ex.getErrorCode()


