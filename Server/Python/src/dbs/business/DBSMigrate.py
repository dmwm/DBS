#!/usr/bin/env python
"""
This module provides dataset migration business object class. 
"""

__revision__ = "$Id: DBSMigrate.py,v 1.16 2010/08/26 21:06:50 yuyi Exp $"
__version__ = "$Revision: 1.16 $"

from WMCore.DAOFactory import DAOFactory

#temporary thing
import json, cjson
import urllib, urllib2
from sqlalchemy import exceptions
from sqlalchemy.exceptions import IntegrityError
from dbs.utils.dbsUtils import dbsUtils

def pprint(a):
    print json.dumps(a, sort_keys=True, indent=4)

class DBSMigrate:
    """ Migration business object class. """

    def __init__(self, logger, dbi, owner):
	
	daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi, owner=owner)
	self.logger = logger
	self.dbi = dbi
	
	self.sm	= daofactory(classname="SequenceManager")
	self.primdslist     = daofactory(classname="PrimaryDataset.List")
	self.datasetlist    = daofactory(classname="Dataset.List")
	self.filelist       = daofactory(classname="File.MgrtList") 
	self.fllist         = daofactory(classname="FileLumi.List")
	self.fplist         = daofactory(classname="FileParent.List")
	self.aelist         = daofactory(classname="AcquisitionEra.List")
	self.pelist         = daofactory(classname="ProcessingEra.List")
	self.mgrlist        = daofactory(classname="MigrationRequests.List")
	self.mgrin          = daofactory(classname="MigrationRequests.Insert")
	self.mgrup          = daofactory(classname="MigrationRequests.Update")
	self.mgrblkin       = daofactory(classname="MigrationBlock.Insert")
	self.blocklist      = daofactory(classname="Block.List")
	self.bparentlist    = daofactory(classname="BlockParent.List")
	self.dsparentlist   = daofactory(classname="DatasetParent.List")
	self.outputCoflist  = daofactory(classname="OutputModuleConfig.List")

    def prepareDatasetMigrationList(self, conn, request):
	"""
	Prepare the ordered lists of blocks based on input dataset
	    1. Get list of blocks from source
	    2. Check and see if these blocks are already at DST
	    3. Check if dataset has parents
	    4. Check if parent blocks are already at DST
	    
	"""
	ordered_dict={}
	order_counter=0 
	srcdataset=request["migration_input"]
	url=request["migration_url"]
	try:
	    tmp_ordered_dict=self.processDatasetBlocks(url, conn, srcdataset, order_counter)	
	    if tmp_ordered_dict != {}:  
		ordered_dict.update(tmp_ordered_dict)
	    # Now process the parent datasets
	    parent_ordered_dict=self.getParentDatastesOrderedList(url, conn, srcdataset, order_counter+1)
	    if parent_ordered_dict != {}:
		ordered_dict.update(parent_ordered_dict)
#	    print ordered_dict
	    return ordered_dict  
	except Exception, ex:
	    raise Exception("Failed to prepare ordered block list of dataset %s for migration, %s" % (srcdataset, str(ex)))

    def processDatasetBlocks(self, url, conn, inputdataset, order_counter):
	"""
	Utility function, that comapraes blocks of a dataset, at source and dst
	    and returns an ordered list of blocks not already at dst for migration
	"""
	ordered_dict={}
	srcblks=self.getSrcBlocks(url, dataset=inputdataset)
	if len(srcblks) < 0:
	    raise Exception("Dataset %s not found at source %s" %(inputdataset, url))
	dstblks=self.blocklist.execute(conn, dataset=inputdataset)
	blocksInSrcNames = [ y['block_name'] for y in srcblks]
	blocksInDstNames = [ x['block_name'] for x in dstblks]
	ordered_dict[order_counter]=[]
	for ablk in blocksInSrcNames:
	    if not ablk in blocksInDstNames:
	        ordered_dict[order_counter].append(ablk)
#	print ordered_dict
	return ordered_dict

    def getParentDatastesOrderedList(self, url, conn, dataset, order_counter):
	"""
	#check if input dataset has parents
	#check if any of the blocks are already at dst
	# prepare the ordered list and return it
	"""
	ordered_dict={}
	parentSrcDatasets=self.getSrcDatasetParents(url, dataset)
	if len(parentSrcDatasets) > 0:
	    parentSrcDatasetNames=[ y['parent_dataset'] for y in parentSrcDatasets]
	    for aparentDataset in parentSrcDatasetNames:
		parent_ordered_dict=self.processDatasetBlocks(url, conn, aparentDataset, order_counter) 
	    	if parent_ordered_dict != {}:
		    ordered_dict.update(parent_ordered_dict)
		# parents of parent
		pparent_ordered_dict=self.getParentDatastesOrderedList(url, conn, aparentDataset, order_counter+1)
		if pparent_ordered_dict != {}:
		    ordered_dict.update(pparent_ordered_dict)
#	print ordered_dict
	return ordered_dict

    def prepareBlockMigrationList(self, conn, request):
	"""
	Prepare the ordered lists of blocks based on input block
	    1. see if block already exists at dst (no need to migrate), raise "ALREADY EXISTS"
	    2. see if block exists at src
	    3. see if block has parents
	    4. see if parent blocks are already at dst
	    5. add 'order' to parent and then this block (ascending)
	    6. return the ordered list
	"""
	ordered_dict={}
	block_name=request["migration_input"]
	url=request["migration_url"]
	order_counter=0	
	try:
	    #1.
	    dstblock = self.blocklist.execute(conn, block_name=block_name )
	    if len(dstblock) > 0:
		raise Exception("BLOCK %s already exists at destination" % block_name)
	    #2.
	    srcblock= self.getSrcBlocks(url, block=block_name)
	    if len(srcblock) < 1:
		raise Exception("BLOCK %s does not exist at source dbs %s" %(url, block_name))
	    ##This block has to be migrated
	    ordered_dict[order_counter]=[]
	    ordered_dict[order_counter].append(block_name)
	    parent_ordered_dict=self.getParentBlocksOrderedList(url, conn, block_name, order_counter+1)
	    if parent_ordered_dict != {}:
		ordered_dict.update(parent_ordered_dict)
	    #6.
	    #print ordered_dict
	    return ordered_dict
        except Exception, ex:
	    raise Exception("Failed to prepare ordered migration list of block %s for migration, %s" % (block_name, str(ex)))
 
    def getParentBlocksOrderedList(self, url, conn, block_name, order_counter):
	    ordered_dict={}
	    #3.
	    parentBlocksInSrc = self.getSrcBlockParents(url, block_name)
	    parentBlocksInSrcNames = [ y['block_name'] for y in parentBlocksInSrc ]
	    #4.
	    if len(parentBlocksInSrcNames) > 0:
		ordered_dict[order_counter]=[]
		# check which of these are already at dst
		# the only way we can do, is to list blocks for parent dataset, and then just check the ones we are interested in
		parent_dataset=parentBlocksInSrcNames[0].split('#')[0]
		parentBlocksInDst=self.blocklist.execute(conn, parent_dataset)
		parentBlocksInDstNames = [ y['block_name'] for y in parentBlocksInDst ]
		for ablockAtSrc in parentBlocksInSrcNames:
		    if ablockAtSrc not in parentBlocksInDstNames: #block is not already at dst
			#5.
			ordered_dict[order_counter].append(ablockAtSrc)
			#Also check if it has parents (recurrsion begins)
			tmp_ordered_dict=self.getParentBlocksOrderedList(url, conn, ablockAtSrc, order_counter+1)
			if tmp_ordered_dict != {}:
			    ordered_dict[order_counter+1]=[]
			    ordered_dict.update(tmp_ordered_dict)
	    return ordered_dict
	    		
    def insertMigrationRequest(self, request):
	"""request kyes: migration_url, migration_input, migration_block, migration_user"""
    
	conn = self.dbi.connection()
	# chek if already queued
	try:
	    alreadyqueued=self.mgrlist.execute(conn, migration_url=request["migration_url"], migration_input=request["migration_input"])
	    if len(alreadyqueued) > 0:
		return {"migration_report" : "REQUEST ALREADY QUEUED", "migration_details" : alreadyqueued[0] }
	except Exception, ex:
	    conn.close()
	    raise Exception("ENQUEUEING_FAILED reason may be ( %s ) " %ex)
	   
	try: 
	    # not already queued	    
	    #Determine if its a dataset or block migration
	    if request["migration_input"].find("#") != -1:
		ordered_list=self.prepareBlockMigrationList(conn, request)
	    else:
		ordered_list=self.prepareDatasetMigrationList(conn, request)
	    # now we have the blocks that need to be queued (ordered)
	except Exception, ex:
	    raise
    	    
	tran = conn.begin()
	try:
	    # insert the request
	    request.update(migration_status=0)
	    request['migration_request_id'] = self.sm.increment(conn, "SEQ_MR", tran)
	    self.mgrin.execute(conn, request, tran)
	    # INSERT the ordered_list
	    totalQueued=0
	    for iter in reversed(range(len(ordered_list))):
		if len(ordered_list[iter]) > 0:
		    daoinput = [ {"migration_block_id" : self.sm.increment(conn, "SEQ_MB", tran), "migration_request_id" : request["migration_request_id"], \
				"migration_block_name" : blk, "migration_order" : iter,
				"migration_status" : 0, "creation_date": dbsUtils().getTime(), \
				"last_modification_date" : dbsUtils().getTime(), "create_by" : dbsUtils().getCreateBy() , \
				"last_modified_by" : 0 } \
						for blk in ordered_list[iter] ]  
		    self.mgrblkin.execute(conn, daoinput, tran)	
		    totalQueued+=len(ordered_list[iter])
	    # all good ?, commit the transaction
	    tran.commit()
	    # return things like (X blocks queued for migration)
	    return {"migration_report" : "REQUEST QUEUED with total %d blocks to be migrated" %totalQueued, "migration_details" : request }
	except exceptions.IntegrityError, ex:
	    tran.rollback()
	    if str(ex).find("unique constraint") != -1 or str(ex).lower().find("duplicate") != -1:
		self.logger.warning("Migration request already exists in DBS")
		request["migration_request_id"] = ""
		return {"migration_report" : "REQUEST ALREADY QUEUED", "migration_details" : request }
	    else:
		self.logger.exception(ex)
	        raise Exception("ENQUEUEING_FAILED reason may be ( %s ) " %ex)  
	except Exception, ex:
	    tran.rollback()
	    raise Exception("ENQUEUEING_FAILED reason may be ( %s ) " %ex)
	finally:
	    conn.close()
    
    def listMigrationRequests(self, migration_request_id="", block_name="", dataset="", user=""):
	"""
	get the status of the migration
	migratee : can be dataset or block_name
	"""
	
	conn = self.dbi.connection()
	migratee=""
	try:
	   if block_name:
		migratee=block_name
	   elif dataset:
		migratee=dataset
	   result = self.mgrlist.execute(conn, migration_url="", migration_input=migratee, create_by=user, migration_request_id="")
	   conn.close()
	   return result
	except:
	    raise
	finally:
	    conn.close()

    def updateMigrationStatus(self, migration_status, migration_dataset):
	conn = self.dbi.connection()
	try:
	    upst = dict(migration_status=migration_status, migration_dataset=migration_dataset)
	    self.mgrup.execute(conn,upst)
	except:
	    raise
	finally:
	    conn.close()

    ##-- below are the actual migration methods

    def dumpBlock(self, block_name):
	""" This method is used at source server and gets the 
	    information on a single block that is being migrated.
	    Try to return in a format to be ready for insert calls"""
        conn = self.dbi.connection()
	try :
	    #block name is unique
	    block1 = self.blocklist.execute(conn, block_name=block_name)
	    if not block1:
		return {}
	    block = block1[0]
	    #a block only has one dataset and one primary dataset
	    #in order to reduce the number of dao objects, we will not write a special
	    #migration one. However, we will have to remove the extrals
	    dataset = self.datasetlist.execute(conn,dataset=block["dataset"])[0]
	    #get block parentage
	    bparent = self.bparentlist.execute(conn, block['block_name'])
	    #get dataset parentage
	    dsparent = self.dsparentlist.execute(conn, dataset['dataset'])
	    for p in dsparent:
		del p['parent_dataset_id'], p['dataset']
	    fparent_list = self.fplist.execute(conn, block_id=block['block_id'])
	    fconfig_list = self.outputCoflist.execute(conn, block_id=block['block_id'])
	    acqEra = {}
	    prsEra = {}
	    if(dataset["acquisition_era_name"] != "" ):
		acqEra  = self.aelist.execute(conn, acquisitionEra=dataset["acquisition_era_name"])[0] 
	    if(dataset["processing_version"] != "" ):
		prsEra  = self.pelist.execute(conn, processingV=dataset["processing_version"])[0]
	    primds = self.primdslist.execute(conn, primary_ds_name=dataset["primary_ds_name"])[0]
	    del dataset["primary_ds_name"], dataset['primary_ds_type']
	    files = self.filelist.execute(conn, block_name=block_name)
	    #import pdb
	    #pdb.set_trace()
	    for f in files:
		#There are a trade off between json sorting and db query. We keep lumi sec in a file,
		#but the file parentage seperate from file
		f.update(file_lumi_list = self.fllist.execute(conn, logical_file_name=f['logical_file_name']))
		         #file_parent_list = self.fplist.execute(conn, logical_file_name=f['logical_file_name']))
                del f['branch_hash_id']
            del dataset["acquisition_era_name"], dataset["processing_version"]
	    del block["dataset"]
	    result= dict(block=block, dataset=dataset, primds=primds, files=files, \
	             block_parent_list=bparent, ds_parent_list=dsparent, \
		     file_conf_list=fconfig_list,   file_parent_list=fparent_list)
	    if acqEra:
		result["acquisition_era"]=acqEra
	    if prsEra:
		result["processing_era"]=prsEra
	    return result
	except:
	    raise
	finally:
	    conn.close()
	
    def callDBSService(self, resturl):
	req = urllib2.Request(url = resturl)
	data = urllib2.urlopen(req)
	ddata = cjson.decode(data.read())
	return ddata
    
    def getSrcDatasetParents(self, url, dataset):
	"""
	List block at src DBS
	"""
	resturl = "%s/datasetparents?dataset=%s" % (url, dataset)
	return self.callDBSService(resturl)
    
    def getSrcBlockParents(self, url, block):
	"""
	List block at src DBS
	"""
	blockname = block.replace("#",urllib.quote_plus('#'))
	resturl = "%s/blockparents?block_name=%s" % (url, blockname)
	return self.callDBSService(resturl)
    
    def getSrcBlocks(self, url, dataset="", block=""):
	"""Need to list all blocks of the dataset and its parents starting from the top
	   For now just list the blocks from this dataset.
	   Client type call..."""
	if block:
	    blockname = block.replace("#",urllib.quote_plus('#'))
	    resturl = "%s/blocks?block_name=%s" % (url,blockname)
	elif dataset:
	    resturl = "%s/blocks?dataset=%s" % (url,dataset)
	else:
	    raise Exception("INVALID block or dataset name")
	return self.callDBSService(resturl)


