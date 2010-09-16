#!/usr/bin/env python
"""
This module provides dataset migration business object class. 
"""

__revision__ = "$Id: DBSMigrate.py,v 1.2 2010/06/24 21:38:51 afaq Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.DAOFactory import DAOFactory

#temporary thing
import json, cjson
import urllib, urllib2
from sqlalchemy import exceptions
from sqlalchemy.exceptions import IntegrityError

def pprint(a):
    print json.dumps(a, sort_keys=True, indent=4)

class DBSMigrate:
    """ Migration business object class. """

    def __init__(self, logger, dbi, owner):
	
	daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi, owner=owner)
	self.logger = logger
	self.dbi = dbi
	
	self.sm	= daofactory(classname="SequenceManager")
	self.mgrlist = daofactory(classname="MigrationRequests.List")
	self.mgrin   = daofactory(classname="MigrationRequests.Insert")
	self.mgrup   = daofactory(classname="MigrationRequests.Update")
	self.blocklist = daofactory(classname="Block.List")





####################### watch for conn management 





	
    def handleBlockMigration(self, conn, request):
	"""
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
	    dstblock = self.blocklist.execute(conn, block_name )
	    if len(dstblock) > 0:
		raise Exception("BLOCK %s already exists at destination" % block_name)
	    #2.
	    srcblock= self.getSrcBlocks(url, block=block_name)
	    if len(srcblock) < 1:
		raise Exception("BLOCK %s does not exist at source dbs %s" %(url, block_name))
	    ##This block has to be migrated
	    ordered_dict[order_counter]=[]
	    ordered_dict[order_counter].append(block_name)
	    parent_ordered_dict=self.getParentOrderedList(url, conn, block_name, order_counter+1)
	    if parent_ordered_dict != {}:
		ordered_dict.update(parent_ordered_dict)
	    #6.
	    return ordered_dict
        except Exception, ex:
	    raise Exception("Failed to queue block %s for migration, %s" % (block_name, str(ex)))
 
    def getParentOrderedList(self, url, conn, block_name, order_counter):
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
			tmp_ordered_dict=self.getParentOrderedList(url, conn, ablockAtSrc, order_counter+1)
			if tmp_ordered_dict != {}:
			    ordered_dict[order_counter+1]=self.getParentOrderedList(url, conn, ablockAtSrc, order_counter+1)
	    return ordered_dict
	    		
    def insertMigrationRequest(self, request):
	"""request kyes: migration_url, migration_input, migration_block, migration_user"""
	conn = self.dbi.connection()
	tran = conn.begin()

	#Determine if its a dataset or block migration
	if request["migration_input"].find("#") != -1:
	    ordered_list=self.handleBlockMigration(conn, request)
	else:
	    ordered_list=self.handleDatasetMigration(conn, request)

	try:
	    request.update(migration_status='PENDING')
	    request['migration_id'] = self.sm.increment(conn, "SEQ_MR", tran)
	    self.mgrin.execute(conn, request, tran)
	    tran.commit()
	    # INSERT the ordered_list
	    

	    # return things like (X blocks queued for migration)
	    return {"migration_status" : "PENDING", "migration_details" : request }

	except exceptions.IntegrityError, ex:
	    tran.rollback()
	    if str(ex).find("unique constraint") != -1 or str(ex).lower().find("duplicate") != -1:
		self.logger.warning("Migration request already exists in DBS")
		request["migration_status"] = "ALREADY_QUEUED"
		request["migration_id"] = ""
		return {"migration_status" : "ALREADY_QUEUED", "migration_details" : request }
	    else:
		self.logger.exception(ex)
	        raise "ENQUEUEING_FAILED reason may be ( %s ) " %ex  
	except Exception, ex:
	    tran.rollback()
	    raise "ENQUEUEING_FAILED reason may be ( %s ) " %ex
	finally:
	    conn.close()
    
    def listMigrationRequests(self, migration_dataset):
	"""get the status of the dataset migration"""
	conn = self.dbi.connection()
	try:
	   result = self.mgrlist.execute(conn, migration_dataset)
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

	block = self.blocklist.execute(conn, block_name=block_name)[0]
	dataset = self.datasetlist.execute(conn,dataset=block["dataset"])[0]
	primds = self.primdslist.execute(conn, primary_ds_name=dataset["primary_ds_name"])[0]
	files = self.filelist.execute(conn, block_name=block_name)
	for f in files:
	    f.update(file_lumi_list = self.fllist.execute(conn, logical_file_name=f['logical_file_name']),
		     file_parent_list = self.fplist.execute(conn, logical_file_name=f['logical_file_name']))
	return dict(block=block, dataset=dataset, primds=primds, files=files)


    def getSrcBlockParents(self, url, block):
	"""
	List block at src DBS
	"""
	blockname = block.replace("#",urllib.quote_plus('#'))
	resturl = "%s/blockparents?block_name=%s" % (url, blockname)
	req = urllib2.Request(url = resturl)
	data = urllib2.urlopen(req)
	ddata = cjson.decode(data.read())
	return ddata 
    
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
	    raise "INVALID block or dataset name"
	req = urllib2.Request(url = resturl)
	data = urllib2.urlopen(req)
	ddata = cjson.decode(data.read())
	return ddata

    def getBlock(self, url, block_name):
	"""Client type call to get the block content from the server"""
	blockname = block_name.replace("#",urllib.quote_plus('#'))
	resturl = "%s/blockdump?block_name=%s" % (url, blockname)
	req = urllib2.Request(url = resturl)
	data = urllib2.urlopen(req)
	ddata = cjson.decode(data.read())
	return ddata


    def putBlock(self, blockcontent):
	"""Huge method that inserts everything.
	   Want to do it in one transaction, so that if there is problem it rolls back.
	   Inserts all data into corresponding tables"""
	conn = self.dbi.connection()
        tran = conn.begin()

	#insert primary dataset
	d = blockcontent["primds"]
	primds = {}
	try:
	    primds["primary_ds_type_id"] = self.primdstpid.execute(conn, d["primary_ds_type"])
	    primds["primary_ds_id"] = self.sm.increment(conn, "SEQ_PDS", tran)
	    for k in ("primary_ds_name", "creation_date", "create_by"):
		primds[k] = d[k]

	    self.primdsin.execute(conn, primds, tran)
	except IntegrityError:
	    pass
	except:
	    tran.rollback()
	    conn.close()
	    raise

	#insert dataset (and processed dataset if it is not already inserted)
	d = blockcontent["dataset"]
	dataset = {}
	dataset["primary_ds_id"]    = primds["primary_ds_id"]
	dataset["data_tier_id"]     = self.tierid.execute(conn, d["data_tier_name"])
	dataset["dataset_type_id"]  = self.datatypeid.execute(conn, d["dataset_type"])
	dataset["physics_group_id"] = self.phygrpid.execute(conn, d["physics_group_name"])

	procid = self.procdsid.execute(conn, d["processed_ds_name"])
	if procid>0:
	    dataset["processed_ds_id"] = procid
	else:
	    procid = self.sm.increment(conn, "SEQ_PSDS", tran)
	    procdaoinput = {"processed_ds_name":d["processed_ds_name"],
                            "processed_ds_id":procid}
	    self.procdsin.execute(conn, procdaoinput, tran)
	    dataset["processed_ds_id"] = procid
        dataset["dataset_id"] = self.sm.increment(conn, "SEQ_DS", tran) 
	for k in ("dataset", "is_dataset_valid", "global_tag", "xtcrosssection",
		  "creation_date", "create_by", "last_modification_date", "last_modified_by"):
	    dataset[k] = d[k]
	try:
	    self.datasetin.execute(conn, dataset, tran)
	except IntegrityError: 
	    pass
	except:
	    tran.rollback()
	    conn.close()
	    raise
	
	#insert files, etc...
	tran.commit()
	conn.close()
	
    def migrate(self, businput):
	""" indata has the following keys:
	    url:
	    dataset:
	"""
	url = businput["url"]
	dataset = businput["dataset"]
	blocks = self.listSrcBlocks(url, dataset)
	for b in blocks:
	    blockcontent = self.getBlock(url, b['block_name'])
	    self.putBlock(blockcontent)
