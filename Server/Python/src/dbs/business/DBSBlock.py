#!/usr/bin/env python
#pylint: disable=C0103
"""
This module provides business object class to interact with Block. 
"""
from WMCore.DAOFactory import DAOFactory
from dbs.utils.dbsUtils import dbsUtils
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
import re

class DBSBlock:
    """
    Block business object class
    """
    def __init__(self, logger, dbi, owner):
        daofactory = DAOFactory(package='dbs.dao', logger=logger, 
                                dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi

        self.blocklist       =    daofactory(classname = "Block.List")
        self.blockbrieflist  =    daofactory(classname = "Block.BriefList")
        self.sm              =    daofactory(classname = "SequenceManager")
        self.datasetid       =    daofactory(classname = "Dataset.GetID")
        self.blockin         =    daofactory(classname = "Block.Insert")
        self.updatestatus    =    daofactory(classname = 'Block.UpdateStatus')
        self.blockparentlist =    daofactory(classname = "BlockParent.List")
        self.blockchildlist  =    daofactory(classname = "BlockParent.ListChild")
        self.blksitein       =    daofactory(classname = "BlockSite.Insert")
        self.datasetlist     =    daofactory(classname = "Dataset.List")
        self.outputCoflist   =    daofactory(classname = "OutputModuleConfig.List")
        self.dsparentlist    =    daofactory(classname = "DatasetParent.List")
        self.fplist          =    daofactory(classname = "FileParent.List")
        self.aelist          =    daofactory(classname = "AcquisitionEra.List")
        self.pelist          =    daofactory(classname = "ProcessingEra.List")
        self.primdslist      =    daofactory(classname = "PrimaryDataset.List")
        self.filelist        =    daofactory(classname = "File.MgrtList")
        self.fllist          =    daofactory(classname = "FileLumi.List")
        

    ##-- dumpBlock is for migration purpose. Moved from DBSMIgration.py to here
    ##-- in oder to avoid depency to pycurl in dbs-web. YG Nov.20, 2012

    def dumpBlock(self, block_name):
        """ This method is used at source server and gets the 
            information on a single block that is being migrated.
            Try to return in a format to be ready for insert calls"""
        if '%' in block_name or '*' in block_name:
            msg = "No wildcard is allowed in block_name for dumpBlock API"
            dbsExceptionHandler('dbsException-invalid-input', msg)

        conn = self.dbi.connection()
        try :
            #block name is unique
            block1 = self.blocklist.execute(conn, block_name=block_name)
            if not block1:
                return {}
            block = block1[0]
            #a block only has one dataset and one primary dataset
            #in order to reduce the number of dao objects, we will not write
            #a special migration one. However, we will have to remove the
            #extras
            dataset1 = self.datasetlist.execute(conn,
                                               dataset=block["dataset"], dataset_access_type="")
            if dataset1:
                dataset = dataset1[0]
                dconfig_list = self.outputCoflist.execute(conn,dataset=dataset['dataset'])
            else: return {}
            #get block parentage
            bparent = self.blockparentlist.execute(conn, block['block_name'])
            #get dataset parentage
            dsparent = self.dsparentlist.execute(conn, dataset['dataset'])
            for p in dsparent:
                del p['parent_dataset_id']
                if 'dataset'in p:
                    del p['dataset']
                elif 'this_dataset' in p:
                    del p['this_dataset']
                else:
                    pass

            fparent_list = self.fplist.execute(conn,
                                               block_id=block['block_id'])
            fconfig_list = self.outputCoflist.execute(conn,
                                                block_id=block['block_id'])
            acqEra = {}
            prsEra = {}
            if dataset["acquisition_era_name"] not in ( "", None):
                acqEra = self.aelist.execute(conn,
                        acquisitionEra=dataset["acquisition_era_name"])[0]
            if dataset["processing_version"] not in ("", None):
                prsEra = self.pelist.execute(conn,
                        processingV=dataset["processing_version"])[0]
            primds = self.primdslist.execute(conn,
                        primary_ds_name=dataset["primary_ds_name"])[0]
            del dataset["primary_ds_name"], dataset['primary_ds_type']
            files = self.filelist.execute(conn, block_name=block_name)
            for f in files:
                #There are a trade off between json sorting and db query.
                #We keep lumi sec in a file, but the file parentage seperate
                #from file
                f.update(file_lumi_list = self.fllist.execute(conn,
                            logical_file_name=f['logical_file_name'], migration=True))
                del f['branch_hash_id']
            del dataset["acquisition_era_name"], dataset["processing_version"]
            del block["dataset"]
            result = dict(block=block, dataset=dataset, primds=primds,
                          files=files, block_parent_list=bparent,
                          ds_parent_list=dsparent, file_conf_list=fconfig_list,
                          file_parent_list=fparent_list, dataset_conf_list=dconfig_list)
            if acqEra:
                result["acquisition_era"] = acqEra
            if prsEra:
                result["processing_era"] = prsEra
            return result
        finally:
            if conn:
                conn.close()

    def updateStatus(self, block_name="", open_for_writing=0):
        """
        Used to toggle the status of a block open_for_writing=1, open for writing, open_for_writing=0, closed
        """
        if not len(block_name) > 1:
            dbsExceptionHandler('dbsException-invalid-input', "DBSBlock/updateStatus. Invalid block_name")
        if open_for_writing not in [1, 0, '1','0']:
            msg = "DBSBlock/updateStatus. open_for_writing can only be 0 or 1 : passed %s."\
                   % open_for_writing 
            dbsExceptionHandler('dbsException-invalid-input', msg)
        conn = self.dbi.connection()
        trans = conn.begin()
        try :
            open_for_writing = int(open_for_writing)
            self.updatestatus.execute(conn, block_name, open_for_writing, dbsUtils().getTime(), trans)
            trans.commit()
            trans = None
        except Exception, ex:
            if trans:
                trans.rollback()
            if conn:conn.close()
            raise ex
        finally:
            if trans:trans.rollback()
            if conn:conn.close()
    
    def listBlockParents(self, block_name=""):
        """
        list parents of a block
        """
        if not block_name:
            msg = " DBSBlock/listBlockParents. Block_name must be provided as a string or a list.\
                No wildcards allowed in block_name/s."
            dbsExceptionHandler('dbsException-invalid-input', msg)
        elif type(block_name) is str:
            if '%' in block_name or '*' in block_name:
                dbsExceptionHandler('dbsException-invalid-input', 'DBSReaderModel/listBlocksParents:\
                    NO WILDCARDS allowed in block_name.')
        elif type(block_name) is list:
            for b in block_name:
                if '%' in b or '*' in b:
                    dbsExceptionHandler('dbsException-invalid-input', 'DBSReaderModel/listBlocksParents:\
                            NO WILDCARDS allowed in block_name.')
        else:
            msg = "DBSBlock/listBlockParents. Block_name must be provided as a string or a list.\
                No wildcards allowed in block_name/s ."
            dbsExceptionHandler('dbsException-invalid-input', msg)
        conn = self.dbi.connection()
        try:
            results = self.blockparentlist.execute(conn, block_name)
            return results
        finally:
            if conn:
                conn.close()

    def listBlockChildren(self, block_name=""):
        """
        list parents of a block
        """
        if (not block_name) or re.search("['%','*']", block_name):
            dbsExceptionHandler('dbsException-invalid-input', "DBSBlock/listBlockChildren. Block_name must be provided." )
        conn = self.dbi.connection()
        try:
            results = self.blockchildlist.execute(conn, block_name)
            return results
        finally:
            if conn:
                conn.close()

    def listBlocks(self, dataset="", block_name="", origin_site_name="",
                   logical_file_name="",run_num=-1, min_cdate=0, max_cdate=0,
                   min_ldate=0, max_ldate=0, cdate=0,  ldate=0, detail=False):
        """
        dataset, block_name, or logical_file_name must be passed.
        """
        if (not dataset) or re.search("['%','*']", dataset):
            if (not block_name) or re.search("['%','*']", block_name):
                if (not logical_file_name) or re.search("['%','*']", logical_file_name) :
                    msg = "DBSBlock/listBlock. You must specify at least one parameter(dataset, block_name, logical_file_name)\
                            with listBlocks api" 
                    dbsExceptionHandler('dbsException-invalid-input', msg)
        try:
            conn = self.dbi.connection()
            dao = (self.blockbrieflist, self.blocklist)[detail]
            result = dao.execute(conn, dataset, block_name, origin_site_name, logical_file_name, run_num,
                                 min_cdate, max_cdate, min_ldate, max_ldate, cdate,  ldate)
            return result
        finally:
            if conn:
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
            dbsExceptionHandler('dbsException-invalid-input', "business/DBSBlock/insertBlock must have block_name and origin_site_name as input")
        conn = self.dbi.connection()
        tran = conn.begin()
        try:
            blkinput = {
                "last_modification_date":businput.get("last_modification_date",  dbsUtils().getTime()),
                "last_modified_by":businput.get("last_modified_by", dbsUtils().getCreateBy()),
                "create_by":businput.get("create_by", dbsUtils().getCreateBy()),
                "creation_date":businput.get("creation_date", dbsUtils().getTime()),
                "open_for_writing":businput.get("open_for_writing", 1),
                "block_size":businput.get("block_size", 0),
                "file_count":businput.get("file_count", 0),
                "block_name":businput.get("block_name"),
                "origin_site_name":businput.get("origin_site_name")
            }
            ds_name = businput["block_name"].split('#')[0]
            blkinput["dataset_id"] = self.datasetid.execute(conn,  ds_name, tran)
            if blkinput["dataset_id"] == -1 : 
                msg = "DBSBlock/insertBlock. Dataset %s does not exists" % ds_name
                dbsExceptionHandler('dbsException-missing-data', msg)
            blkinput["block_id"] =  self.sm.increment(conn, "SEQ_BK", tran)
            self.blockin.execute(conn, blkinput, tran)

            tran.commit()
            tran = None
        except Exception, e:
            if str(e).lower().find("unique constraint") != -1 or str(e).lower().find("duplicate") != -1:
                pass
            else:
                if tran:
                    tran.rollback()
                if conn: conn.close()
                raise
                
        finally:
            if tran:
                tran.rollback()
            if conn:
                conn.close()
