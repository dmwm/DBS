#!/usr/bin/env python
"""
This module provides business object class to interact with File. 
"""

__revision__ = "$Id: DBSFile.py,v 1.16 2010/01/07 17:30:42 afaq Exp $"
__version__ = "$Revision: 1.16 $"

from WMCore.DAOFactory import DAOFactory
from sqlalchemy import exceptions

class DBSFile:
    """
    File business object class
    """
    def __init__(self, logger, dbi, owner):
        daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi

        self.filelist = daofactory(classname="File.List")
        self.sm = daofactory(classname = "SequenceManager")
        self.filein = daofactory(classname = "File.Insert")
        self.flumiin = daofactory(classname = "FileLumi.Insert")
        self.fparentin = daofactory(classname = "FileParent.Insert")
        self.fileid = daofactory(classname = "File.GetID")
        self.datasetid = daofactory(classname = "Dataset.GetID")
        self.blockid = daofactory(classname = "Block.GetID")
        self.blocklist = daofactory(classname = "Block.List")
        self.ftypeid = daofactory(classname = "FileType.GetID")
	self.fpbdlist = daofactory(classname = "FileParentBlock.List")
	self.blkparentin = daofactory(classname = "BlockParent.Insert")
	self.dsparentin = daofactory(classname = "DatasetParent.Insert")
	self.blkstats = daofactory(classname = "Block.ListStats")
	self.blkstatsin = daofactory(classname = "Block.UpdateStats")
	self.outconfigid = daofactory(classname='OutputModuleConfig.GetID')
	self.fconfigin = daofactory(classname='FileOutputMod_config.Insert')
	
    def listFiles(self, dataset="", block_name="", logical_file_name=""):
        """
        either dataset(and lfn pattern) or block(and lfn pattern) must be specified.
        """
        return self.filelist.execute(dataset, block_name, logical_file_name)

    def insertFile(self, businput):
	"""
	This method supports bulk insert of files
	performing other operations such as setting Block and Dataset parentages, 
	setting mapping between OutputConfigModules and File(s) etc.

        logical_file_name (required) : string  <br />
        is_file_valid: (optional, default = 1): 1/0 <br />
        block, required: /a/b/c#d <br />
        dataset, required: /a/b/c <br />
        file_type (optional, default = EDM): one of the predefined types, <br />
        check_sum (optional, default = '-1'): string <br />
        event_count (optional, default = -1): int <br />
        file_size (optional, default = -1.): float <br />
        adler32 (optional, default = ''): string <br />
        md5 (optional, default = ''): string <br />
        auto_cross_section (optional, default = -1.): float <br />
            file_lumi_list (optional, default = []): [{"run_num": 123, "lumi_section_num": 12},{}....] <br />
            file_parent_list(optional, default = []) :[{"file_parent_lfn": "mylfn"},{}....] <br />
            file_assoc_list(optional, default = []) :[{"file_parent_lfn": "mylfn"},{}....] <br />
            file_output_config_list(optional, default = []) :[{"app_name":..., "release_version":..., "pset_hash":...., output_module_label":...},{}.....] <br />
	"""
	conn = self.dbi.connection()
	tran = conn.begin()

	try:
	    # AA- 01/06/2010 -- we have to do this file-by-file, there is no real good way to do this complex operation otherwise 
	    files2insert = []
            fparents2insert = []
            flumis2insert = []
	    fconfigs2insert = []
	    fidl=[]
	    fileInserted=False
	    
	    firstfile = businput[0]
	    # first check if the dataset exists
	    # and block exists that files are suppose to be going to and is OPEN for writing
	    dataset_id = self.datasetid.execute(firstfile["dataset"], conn, True)
	    block_info = self.blocklist.execute(block_name=firstfile["block"])
	    assert len(block_info)==1
	    block_info=block_info[0]
	    assert block_info["block_id"]
	    assert block_info["open_for_writing"]==1
	    block_id = block_info["block_id"]
	    
	    file_type_id = self.ftypeid.execute( firstfile.get("file_type", "EDM"), conn, True)
	    iFile = 0
	    fileIncrement = 40
	    fID = self.sm.increment("SEQ_FL", conn, True)
	    #looping over the files, everytime create a new object 'filein' as you never know 
	    #whats in the original object and we do not want to know
	    for f in businput:
		# create the file object from the original 
		# taking care of defaults, and required
		filein={
		    "logical_file_name" : f["logical_file_name"],
		    "is_file_valid" : f.get("is_file_valid", 1),
		    "check_sum" : f.get("check_sum", -1), 
		    "event_count" : f.get("event_count", -1), 
		    "file_size" : f.get("file_size", -1),
		    "adler32" : f.get("adler32", ""), 
		    "md5" : f.get("md5", ""),
		    "auto_cross_section" : f.get("auto_cross_section", -1),
		    "creation_date" : f["creation_date"], 
		    "create_by": f["create_by"],
		    "last_modification_date": f["last_modification_date"], 
		    "last_modified_by" : f["last_modified_by"] 
		}
		if iFile == fileIncrement:
		    fID = self.sm.increment("SEQ_FL", conn, True)
		    iFile = 0
		filein["file_id"] = fID + iFile
		iFile += 1
		filein["dataset_id"]=dataset_id
		filein["block_id"]=block_id
		filein["file_type_id"]=file_type_id
		#FIXME: Add this later if f.get("branch_hash", "") not in ("", None): 
		#filein["branch_hash"]=self.fbranchid.execute( f.get("branch_hash"), conn, True)

		# insert file  -- as decided, one file at a time
		# filein will be what goes into database
		try:
		    self.filein.execute(filein, conn, True)
		    fileInserted=True
		except exceptions.IntegrityError, ex:
		    if str(ex).find("unique constraint") != -1 :
			#refresh the file_id from database
			#filein["file_id"]=self.fileid.execute(filein["logical_file_name"], conn, True)
			# Lets move on to NEXT file, we do not want to continue processing this file
			self.logger.warning("File already exists in DBS, not touching it: %s" %filein["logical_file_name"])
			continue
		    else:
			raise	
	        # Saving the id for later use
	        files2insert.append(filein)
	        fidl.append(filein["file_id"])

		#Now let us process, file parents, file lumi, file outputmodconfigs, association 

		#file lumi sections
		if f.has_key("file_lumi_list"):
		    fllist = f["file_lumi_list"]
		    if(len(fllist) > 0):
			iLumi = 0
			flIncrement = 1000
			flID = self.sm.increment("SEQ_FLM", conn, True)
			for fl in fllist:
			    if iLumi == flIncrement:
				flID =  self.sm.increment("SEQ_FLM", conn, True)
				iLumi = 0
			    fldao={ 
				"run_num" : fl["run_num"],
				"lumi_section_num" : fl["lumi_section_num"] 
			    }
			    fldao["file_lumi_id"] = flID + iLumi
			    iLumi += 1
			    fldao["file_id"] = filein["file_id"]
			    flumis2insert.append(fldao)
    
		if f.has_key("file_parent_list"):
		    #file parents    
		    fplist = f["file_parent_list"]
		    if(len(fplist) > 0):
			iParent = 0
			fpIncrement = 120
			fpID = self.sm.increment("SEQ_FP", conn, True)
                    
			for fp in fplist:
			    if iParent == fpIncrement:
				fpID = self.sm.increment("SEQ_FP", conn, True)
				iParent  = 0
			    fpdao={}
			    fpdao["file_parent_id"] = fpID + iParent
			    iParent += 1 
			    fpdao["this_file_id"] = filein["file_id"]
			    lfn = fp["file_parent_lfn"]
			    fpdao["parent_file_id"] = self.fileid.execute(lfn, conn, True)
			    fparents2insert.append(fpdao)

		if f.has_key("file_output_config_list"):
		    #file output config modules
		    foutconfigs = f["file_output_config_list"]
		    if(len(foutconfigs) > 0):
			iConfig = 0
			fconfigInc = 5
			fcID = self.sm.increment("SEQ_FC", conn, True)
			for fc in foutconfigs:
			    if iConfig == fconfigInc:
				fcID = self.sm.increment("SEQ_FC", conn, True)
				iConfig = 0
			    fcdao={}
			    fcdao["file_output_config_id"] = fcID + iConfig
			    iConfig += 1
			    fcdao["file_id"] = filein["file_id"]
			    fcdao["output_mod_config_id"]= self.outconfigid.execute(fc["app_name"], \
				                        fc["release_version"], fc["pset_hash"], conn, True)
			    fconfigs2insert.append(fcdao)
			
		#FIXME: file associations?-- in a later release
		#
		# insert file - lumi   
		if flumis2insert:
		    self.flumiin.execute(flumis2insert, conn, True)
		# insert file parent mapping
		if fparents2insert:
		    self.fparentin.execute(fparents2insert, conn, True)
		# insert output module config mapping
		if fconfigs2insert:
		    self.fconfigin.execute(fconfigs2insert, conn, True)    

	    # List the parent blocks and datasets of the file's parents (parent of the block and dataset)
	    # fpbdlist, returns a dict of {block_id, dataset_id} combination
	    if fileInserted:
		fpblks=[]
		fpds=[]
		fileParentBlocksDatasets = self.fpbdlist.execute(fidl, conn, True)
		for adict in fileParentBlocksDatasets:
		    if adict["block_id"] not in fpblks:
			fpblks.append(adict["block_id"])
		    if adict["dataset_id"] not in fpds:
		    	fpds.append(adict["dataset_id"])
		# Update Block parentage
		if len(fpblks) > 0 :
		    # we need to bulk this, number of parents can get big in rare cases
		    bpdaolist=[]
		    iPblk = 0
		    fpblkInc = 10
		    bpID = self.sm.increment("SEQ_BP", conn, True)
		    for ablk in fpblks:
			if iPblk == fpblkInc:
			    bpID = self.sm.increment("SEQ_BP", conn, True)
			    iPblk = 0
			bpdao={ "this_block_id": block_id }
			bpdao["parent_block_id"] = ablk
			bpdao["block_parent_id"] = bpID
			bpdaolist.append(bpdao)
		    # insert them all
		    # Do this one by one, as its sure to have duplicate in dest table
		    for abp in bpdaolist:
			try:
			    self.blkparentin.execute(abp, conn, True)
			except exceptions.IntegrityError, ex:
			    if str(ex).find("unique constraint") != -1 :
				pass
			    else:
				raise
		# Update dataset parentage
		if len(fpds) > 0 :
		    dsdaolist=[]
		    iPds = 0
		    fpdsInc = 10
		    pdsID = self.sm.increment("SEQ_DP", conn, True)
		    for ads in fpds:
			if iPds == fpdsInc:
			    pdsID = self.sm.increment("SEQ_DP", conn, True)
			    iPds = 0
			dsdao={ "this_dataset_id": dataset_id }
			dsdao["parent_dataset_id"] = ads
			dsdao["dataset_parent_id"] = pdsID # PK of table 
			dsdaolist.append(dsdao)
		    # Do this one by one, as its sure to have duplicate in dest table
		    for adsp in dsdaolist:
			try:
			    self.dsparentin.execute(adsp, conn, True)
			except exceptions.IntegrityError, ex:
			    if str(ex).find("unique constraint") != -1 :
				pass
			    else:
				raise

		# Update block parameters, file_count, block_size
		blkParams=self.blkstats.execute(block_id, conn, True)
		self.blkstatsin.execute(blkParams, conn, True)

	    # All good ?. 
            tran.commit()

	except Exception, e:
	    tran.rollback()
	    self.logger.exception(e)
	    raise

	finally:
	    conn.close()
 
