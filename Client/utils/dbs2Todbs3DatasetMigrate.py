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
			self.outputconfs=[]
			self.sitelist=[]

        	def startElement(self, name, attrs):
			if name == 'primary_dataset':
				self.primary_dataset=attrs.get('primary_name')
				self.prdsobj =  { "primary_ds_name" : str(self.primary_dataset), "primary_ds_type": "test" }
				
			if name == 'processed_dataset':
				self.dataset=	{
						"is_dataset_valid": 1 , "primary_ds_name": self.primary_dataset, "primary_ds_type": "test", 
						"dataset_type":"PRODUCTION",
						"global_tag": attrs.get('global_tag'),"xtcrosssection":123,"physics_group_name": "Tracker", 
						#"processing_version" : "1", 
						#"acquisition_era_name" : attrs.get('acquisition_era')
						"processed_ds_name": attrs.get('processed_datatset_name'),
						}
					
				self.processed_dataset=attrs.get('processed_datatset_name')

			if name == 'path':
				self.data_tier = str(attrs.get('dataset_path')).split('/')[3]
				self.path=attrs.get('dataset_path')
				self.dataset["data_tier_name"]=self.data_tier 
				self.dataset["dataset"]=self.path

			if name == 'block':
				self.block  = {
						"block_name":attrs.get('name'), "open_for_writing":1,"block_size": attrs.get('size'), 
						"file_count":attrs.get('number_of_files'), "creation_date":attrs.get('creation_date'), 
						"create_by":attrs.get('created_by'), "last_modification_date":attrs.get('last_modification_date'), 
						"last_modified_by":attrs.get('last_modified_by')
						}
				self.block_name=attrs.get('name')

			if name == 'storage_element':
				self.block["origin_site"]=attrs.get('storage_element_name')
				self.sitelist.append({ "site_name" : attrs.get('storage_element_name') } )

			if name == 'file':
				self.currfile={
					"logical_file_name":attrs.get('lfn'), "is_file_valid": 1, "dataset": self.path, "block" : self.block_name,
					"file_type": "EDM",
					"check_sum": attrs.get('checksum'), "event_count": attrs.get('number_of_events'), "file_size": attrs.get('size'), 
					"adler32": attrs.get('adler32'), "md5": attrs.get('md5'), "auto_cross_section": 0.0, 
					"create_by":attrs.get('created_by'), "last_modification_date":attrs.get('last_modification_date'),
                                        "last_modified_by":attrs.get('last_modified_by')
				     }

			if name == 'file_lumi_section':
				filelumi={
						"run_num":attrs.get('run_number'),
						"lumi_section_num":attrs.get('lumi_section_number')
					}
				self.currfilelumis.append(filelumi)
			
			if name == 'file_parent':
				fileparent = {
						"file_parent_lfn":attrs.get('lfn')
					}
				self.currfileparents.append(fileparent)

			if name == 'algorithm':
                                outputconf = {
				    "app_name" : attrs.get('app_executable_name'),
				    "release_version" : attrs.get('app_version'),
				    "pset_hash" : attrs.get('ps_hash'),
				    "output_module_label" : attrs.get('app_family_name')
				}
				self.outputconfs.append(outputconf)

			"""
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
				self.currfile["file_lumi_list"]=self.currfilelumis
				self.currfile["file_parent_list"]=self.currfileparents
				self.currfile["file_output_config_list"]=self.outputconfs
				self.currfilelumis=[]
				self.currfileparents=[]
				self.files.append(self.currfile)

                        if name == 'dbs':
				try :
					# Lets populate this in DBS
        				# API Object  
        				#print self.prdsobj
        				dbs3api.insertPrimaryDataset(self.prdsobj)

					for anocfg in self.outputconfs:
					    dbs3api.insertOutputConfig(anocfg)

					##print self.dataset
					self.dataset["output_configs"]=self.outputconfs
        				dbs3api.insertDataset(self.dataset)

					for asite in self.sitelist:
					    dbs3api.insertSite(asite)
    
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
						if file.has_key('file_lumi_list'):
							block_time['block_weight']+=long(len(file['file_lumi_list']))
							block_time['file_lumi_section_count']+=long(len(file['file_lumi_list']))
						if file.has_key('file_parent_list'):
							block_time['block_weight']+=long(len(file['file_parent_list']))
							block_time['file_parent_count']+=long(len(file['file_parent_list']))
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


