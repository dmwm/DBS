#!/usr/bin/env python
"""
This module provides dataset migration business object class. 
"""

__revision__ = "$Id: DBSMigrate.py,v 1.1 2010/04/22 07:48:37 akhukhun Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.DAOFactory import DAOFactory

#temporary thing
import json, cjson
import urllib, urllib2
from sqlalchemy.exceptions import IntegrityError

def pprint(a):
    print json.dumps(a, sort_keys=True, indent=4)

class DBSMigrate:
    """ Migration business object class. """

    def __init__(self, logger, dbi, owner):
	
	daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi, owner=owner)
	self.logger = logger
	self.dbi = dbi

	self.mgrlist = daofactory(classname="MigrationRequests.List")
	self.mgrin   = daofactory(classname="MigrationRequests.Insert")
	self.mgrup   = daofactory(classname="MigrationRequests.Update")

        self.sm		    = daofactory(classname="SequenceManager")
        self.primdslist	    = daofactory(classname="PrimaryDataset.List")
	self.datasetlist    = daofactory(classname="Dataset.List")
	self.blocklist	    = daofactory(classname="Block.List")
	self.filelist	    = daofactory(classname="File.List")
	self.fplist	    = daofactory(classname="FileParent.List")
        self.fllist	    = daofactory(classname="FileLumi.List")
	self.primdstpid	    = daofactory(classname='PrimaryDSType.GetID')
	self.tierid	    = daofactory(classname='DataTier.GetID')
        self.datatypeid	    = daofactory(classname='DatasetType.GetID')
        self.phygrpid	    = daofactory(classname='PhysicsGroup.GetID')
        self.procdsid	    = daofactory(classname='ProcessedDataset.GetID')
        self.procdsin	    = daofactory(classname='ProcessedDataset.Insert')
        self.primdsin	    = daofactory(classname="PrimaryDataset.Insert")
        self.datasetin	    = daofactory(classname='Dataset.Insert')
	

    
    def insertMigrationRequest(self, request):
	"""request kyes: migration_url, migration_dataset"""
	conn = self.dbi.connection()
	tran = conn.begin()
	try:
	    request.update(migration_status='PENDING')
	    request['migration_id'] = self.sm.increment(conn, "SEQ_MR", tran)
	    self.mgrin.execute(conn, request, tran)
	    tran.commit()
	except Exception, ex:
	    tran.rollback()
	    self.logger.exception(ex)
	    raise
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


    def listBlocks(self, url, dataset):
	"""Need to list all blocks of the dataset and its parents starting from the top
	   For now just list the blocks from this dataset.
	   Client type call..."""
	resturl = "%s/DBS/blocks?dataset=%s" % (url,dataset)
	req = urllib2.Request(url = resturl)
	data = urllib2.urlopen(req)
	ddata = cjson.decode(data.read())
	return ddata

    def getBlock(self, url, block_name):
	"""Client type call to get the block content from the server"""
	blockname = block_name.replace("#",urllib.quote_plus('#'))
	resturl = "%s/DBS/blockdump?block_name=%s" % (url, blockname)
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
	blocks = self.listBlocks(url, dataset)
	for b in blocks:
	    blockcontent = self.getBlock(url, b['block_name'])
	    self.putBlock(blockcontent)
