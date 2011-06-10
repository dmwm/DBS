#!/usr/bin/env python
"""
This module provides business object class to interact with Block. 
"""
from WMCore.DAOFactory import DAOFactory
from dbs.utils.dbsUtils import dbsUtils
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.dbsException import dbsException,dbsExceptionCode
from sqlalchemy import exceptions
import re

class DBSBlock:
    """
    Block business object class
    """
    def __init__(self, logger, dbi, owner):
        daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi

        self.blocklist = daofactory(classname="Block.List")
        self.blockbrieflist = daofactory(classname="Block.BriefList")

        self.sm = daofactory(classname = "SequenceManager")
        self.datasetid = daofactory(classname = "Dataset.GetID")
        self.blockin = daofactory(classname = "Block.Insert")
	self.updatestatus = daofactory(classname='Block.UpdateStatus')
        self.blockparentlist = daofactory(classname="BlockParent.List")
        self.blockchildlist = daofactory(classname="BlockParent.ListChild")
        self.blksitein = daofactory(classname = "BlockSite.Insert")
	
    def updateStatus(self, block_name="", open_for_writing=0):
	"""
	Used to toggle the status of a block open_for_writing=1, open for writing, open_for_writing=0, closed
	"""
	if not len(block_name) > 1:
	    msg="%s DBSBlock/updateStatus. Invalid block_name" %dbsExceptionCode['dbsException-invalid-input']
            dbsExceptionHandler('dbsException-invalid-input', msg)
	if open_for_writing not in [1, 0, '1','0']:
	    msg = "%s DBSBlock/updateStatus. open_for_writing can only be 0 or 1 : passed %s."\
                   %(dbsExceptionCode['dbsException-invalid-input'],  open_for_writing) 
            dbsExceptionHandler('dbsException-invalid-input', msg)
        conn = self.dbi.connection()
        trans = conn.begin()
	try :
            open_for_writing=int(open_for_writing)
	    self.updatestatus.execute(conn, block_name, open_for_writing, dbsUtils().getTime(), trans)
	    trans.commit()
	except Exception, ex:
	    trans.rollback()
	    raise ex
	finally:
	    trans.close()
	    conn.close()
    
    def listBlockParents(self, block_name=""):
	"""
	list parents of a block
	"""
        if not block_name:
            msg = " %s DBSBlock/listBlockParents. Block_name must be provided as a string or a list.\
                No wildcards allowed in block_name/s."%dbsExceptionCode['dbsException-invalid-input']
        elif type(block_name) is str:
            if '%' in block_name or '*' in block_name:
                dbsExceptionHandler('dbsException-invalid-input', '%s.DBSReaderModel/listBlocksParents:\
                    NO WILDCARDS allowed in block_name.'%dbsExceptionCode['dbsException-invalid-input'])
        elif type(block_name) is list:
            for b in block_name:
                if '%' in b or '*' in b:
                        dbsExceptionHandler('dbsException-invalid-input', '%s.DBSReaderModel/listBlocksParents:\
                            NO WILDCARDS allowed in block_name.'%dbsExceptionCode['dbsException-invalid-input'])
        else:
            msg = " %s DBSBlock/listBlockParents. Block_name must be provided as a string or a list.\
                No wildcards allowed in block_name/s ."%dbsExceptionCode['dbsException-invalid-input']
            dbsExceptionHandler('dbsException-invalid-input', msg)
	try:
	    conn = self.dbi.connection()
	    results = self.blockparentlist.execute(conn, block_name)
	    return results
	except Exception, ex:
	    raise ex
	finally:
	    conn.close()

    def listBlockChildren(self, block_name=""):
	"""
	list parents of a block
	"""
        if (not block_name) or re.search('^%%*$', block_name):
            msg = " %s DBSBlock/listBlockChildren. Block_name must be provided.\n" %dbsExceptionCode['dbsException-invalid-input'] 
            dbsExceptionHandler('dbsException-invalid-input', msg)
	try:
	    conn = self.dbi.connection()
	    results = self.blockchildlist.execute(conn, block_name)
	    return results
	except Exception, ex:
	    raise ex
	finally:
	    conn.close()

    def listBlocks(self, dataset="", block_name="", origin_site_name="", logical_file_name="",run_num=-1, 
                   min_cdate=0, max_cdate=0, min_ldate=0, max_ldate=0, cdate=0,  ldate=0, detail=False):
        """
        dataset, block_name, or logical_file_name must be passed.
        """
	if (not dataset) or re.search('^%%*$', dataset):
	    if (not block_name) or re.search('^%%*$', block_name):
		if (not logical_file_name) or re.search('^%%*$', logical_file_name) :
			msg = "%s DBSBlock/listBlock. You must specify at least one parameter(dataset, block_name, logical_file_name)\
                            with listBlocks api"  %dbsExceptionCode['dbsException-invalid-input']
                        dbsExceptionHandler('dbsException-invalid-input', msg)
	try:
	    conn = self.dbi.connection()
	    dao = (self.blockbrieflist, self.blocklist)[detail]
	    result = dao.execute(conn, dataset, block_name, origin_site_name, logical_file_name, run_num,
                                 min_cdate, max_cdate, min_ldate, max_ldate, cdate,  ldate)
	    return result
        except Exception, ex:
	    raise ex
        finally:
	    conn.close()
    
    
    def insertBlock(self, businput):
        """
        Input dictionary has to have the following keys:
        blockname
	
        It may have:
        open_for_writing, origin_site(name), block_size,
        file_count, creation_date, create_by, last_modification_date, last_modified_by
        
        it builds the correct dictionary for dao input and executes the dao

        NEED to validate there are no extra keys in the businput
        """
        if not (businput.has_key("block_name") and businput.has_key("origin_site_name")  ):
            msg = "%s business/DBSBlock/insertBlock must have block_name and origin_site_name as input" %dbsExceptionCode['dbsException-invalid-input']
            dbsExceptionHandler('dbsException-invalid-input', msg)
        conn = self.dbi.connection()
        tran = conn.begin()
        try:
	    blkinput = {
		"last_modification_date":businput.get("last_modification_date", None),
		"last_modified_by":businput.get("last_modified_by", None),
		"create_by":businput.get("create_by", None),
		"creation_date":businput.get("creation_date", None),
		"open_for_writing":businput.get("open_for_writing", None),
		"block_size":businput.get("block_size", None),
		"file_count":businput.get("file_count", None),
		"block_name":businput.get("block_name", None)
	    }
	    ds_name = businput["block_name"].split('#')[0]
            blkinput["dataset_id"] = self.datasetid.execute(conn,  ds_name, tran)
	    if blkinput["dataset_id"] == -1 : 
                msg = "%s DBSBlock/insertBlock. Dataset %s does not exists"\
                    %(dbsExceptionCode['dbsException-missing-data'], ds_name)
                dbsExceptionHandler('dbsException-missing-data', msg)
            blkinput["block_id"] =  self.sm.increment(conn, "SEQ_BK", tran)
            if(businput.has_key("origin_site_name")):
		blkinput["origin_site_name"] = businput["origin_site_name"]
            self.blockin.execute(conn, blkinput, tran)

            tran.commit()
        except Exception, e:
	    if str(e).lower().find("unique constraint") != -1 or str(e).lower().find("duplicate") != -1:
		pass
	    else:
		tran.rollback()
		raise
		
        finally:
            conn.close()
