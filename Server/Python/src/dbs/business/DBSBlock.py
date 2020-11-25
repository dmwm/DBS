#!/usr/bin/env python
#pylint: disable=C0103
"""
This module provides business object class to interact with Block. 
"""
from WMCore.DAOFactory import DAOFactory
from dbs.utils.dbsUtils import dbsUtils
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
import re


def convertByteStr(unicodeStr):
    """
    Utilitarian function which converts an unicode string to
    an 8-bit string.
    """
    if isinstance(unicodeStr, basestring):
        return unicodeStr.encode("ascii")
    return unicodeStr

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
        self.updatesitename  =    daofactory(classname = 'Block.UpdateSiteName')
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
        self.bkOriginlist    =    daofactory(classname = "Block.ListBlockOrigin")
        

    ##-- dumpBlock is for migration purpose. Moved from DBSMIgration.py to here
    ##-- in oder to avoid depency to pycurl in dbs-web. YG Nov.20, 2012

    def dumpBlock(self, block_name):
        """ This method is used at source server and gets the 
            information on a single block that is being migrated.
            Try to return in a format to be ready for insert calls"""
        if '%' in block_name or '*' in block_name:
            msg = "No wildcard is allowed in block_name for dumpBlock API"
            dbsExceptionHandler('dbsException-invalid-input', msg, self.logger.exception)

        block_name = convertByteStr(block_name)
        conn = self.dbi.connection()
        try :
            #block name is unique
            block1 = self.blocklist.execute(conn, block_name=block_name)
            block = []
            for b1 in block1:
                if not b1:
                    return {}
                else:
                    block = b1
            #a block only has one dataset and one primary dataset
            #in order to reduce the number of dao objects, we will not write
            #a special migration one. However, we will have to remove the
            #extras
            #block1 is a generator. When it is empty, it will skip the for loop above. why? 
            #we cannot test on b1 to decide if the generator is empty or not.
            #so have to do below:
            if not block: return {}
            dataset1 = self.datasetlist.execute(conn, dataset=convertByteStr(block["dataset"]),
                                                dataset_access_type="")
            dataset = []
            for d in dataset1:
                if d:
                    dataset = d
                    dconfig_list = self.outputCoflist.execute(conn, dataset=convertByteStr(dataset['dataset']))
                else: return {}
            #get block parentage
            bparent = self.blockparentlist.execute(conn, convertByteStr(block['block_name']))
            #get dataset parentage
            dsparent = self.dsparentlist.execute(conn, convertByteStr(dataset['dataset']))
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
            fparent_list2 = []
            for fp in fparent_list:
                fparent_list2.append(fp)
            #print "---YG file Parent List--"
            #print fparent_list2
            fconfig_list = self.outputCoflist.execute(conn,
                                                block_id=block['block_id'])
            acqEra = {}
            prsEra = {}
            if dataset["acquisition_era_name"] not in ( "", None):
                acqEra = self.aelist.execute(conn,
                        acquisitionEra=convertByteStr(dataset["acquisition_era_name"]))[0]
            if dataset["processing_version"] not in ("", None):
                prsEra = self.pelist.execute(conn,
                        processingV=convertByteStr(dataset["processing_version"]))[0]
            primds = self.primdslist.execute(conn,
                        primary_ds_name=convertByteStr(dataset["primary_ds_name"]))[0]
            del dataset["primary_ds_name"], dataset['primary_ds_type']
            files = self.filelist.execute(conn, block_name=block_name)
            for f in files:
                #There are a trade off between json sorting and db query.
                #We keep lumi sec in a file, but the file parentage seperate
                #from file
                file_lumi_list = []
                for item in self.fllist.execute(conn, logical_file_name=convertByteStr(f['logical_file_name']),
                                                migration=True):
                    file_lumi_list.append(item)
                #print "---YG file lumi list---"
                f.update(file_lumi_list = file_lumi_list)
                del file_lumi_list #YG 09/2015
                del f['branch_hash_id']
            del dataset["acquisition_era_name"], dataset["processing_version"]
            del block["dataset"]
            result = dict(block=block, dataset=dataset, primds=primds,
                          files=files, block_parent_list=bparent,
                          ds_parent_list=dsparent, file_conf_list=fconfig_list,
                          file_parent_list=fparent_list2, dataset_conf_list=dconfig_list)
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
        if open_for_writing not in [1, 0, '1', '0']:
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
        except Exception as ex:
            if trans:
                trans.rollback()
            if conn:conn.close()
            raise ex
        finally:
            if conn:conn.close()

    def updateSiteName(self, block_name, origin_site_name):
        """
        Update the origin_site_name for a given block name
        """
        if not origin_site_name:
            dbsExceptionHandler('dbsException-invalid-input',
                                "DBSBlock/updateSiteName. origin_site_name is mandatory.")
        conn = self.dbi.connection()
        trans = conn.begin()
        try:
            self.updatesitename.execute(conn, block_name, origin_site_name)
        except:
            if trans:
                trans.rollback()
            raise
        else:
            if trans:
                trans.commit()
        finally:
            if conn:
                conn.close()
    
    def listBlockParents(self, block_name=""):
        """
        list parents of a block
        """
        if not block_name:
            msg = " DBSBlock/listBlockParents. Block_name must be provided as a string or a list. \
                No wildcards allowed in block_name/s."
            dbsExceptionHandler('dbsException-invalid-input', msg)
        elif isinstance(block_name, basestring):
            try:
                block_name = str(block_name)
                if '%' in block_name or '*' in block_name:
                    dbsExceptionHandler("dbsException-invalid-input", "DBSReaderModel/listBlocksParents: \
                    NO WILDCARDS allowed in block_name.")
            except:
                dbsExceptionHandler("dbsException-invalid-input", "DBSBlock/listBlockParents. Block_name must be \
                provided as a string or a list. No wildcards allowed in block_name/s .")
        elif type(block_name) is list:
            for b in block_name:
                if '%' in b or '*' in b:
                    dbsExceptionHandler("dbsException-invalid-input", "DBSReaderModel/listBlocksParents: \
                            NO WILDCARDS allowed in block_name.")
        else:
            msg = "DBSBlock/listBlockParents. Block_name must be provided as a string or a list. \
                No wildcards allowed in block_name/s ."
            dbsExceptionHandler("dbsException-invalid-input", msg)
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
            dbsExceptionHandler("dbsException-invalid-input", "DBSBlock/listBlockChildren. Block_name must be provided." )
        conn = self.dbi.connection()
        try:
            results = self.blockchildlist.execute(conn, block_name)
            return results
        finally:
            if conn:
                conn.close()

    def listBlocks(self, dataset="", block_name="", data_tier_name="", origin_site_name="",
                   logical_file_name="", run_num=-1, min_cdate=0, max_cdate=0,
                   min_ldate=0, max_ldate=0, cdate=0,  ldate=0, open_for_writing=-1, detail=False):
        """
        dataset, block_name, data_tier_name or logical_file_name must be passed.
        """
        if (not dataset) or re.search("['%','*']", dataset):
            if (not block_name) or re.search("['%','*']", block_name):
                if (not logical_file_name) or re.search("['%','*']", logical_file_name):
                    if not data_tier_name or re.search("['%','*']", data_tier_name):
                        msg = "DBSBlock/listBlock. You must specify at least one parameter(dataset, block_name,\
			       	data_tier_name, logical_file_name) with listBlocks api"
                        dbsExceptionHandler('dbsException-invalid-input2', msg, self.logger.exception, msg)

        if data_tier_name:
            if not (min_cdate and max_cdate) or (max_cdate-min_cdate)>32*24*3600:
                msg = "min_cdate and max_cdate are mandatory parameters. If data_tier_name parameter is used \
                       the maximal time range allowed is 31 days"
                dbsExceptionHandler('dbsException-invalid-input2', msg, self.logger.exception, msg)
            if detail:
                msg = "DBSBlock/listBlock. Detail parameter not allowed togther with data_tier_name"
                dbsExceptionHandler('dbsException-invalid-input2', msg, self.logger.exception, msg)

        with self.dbi.connection() as conn:
            dao = (self.blockbrieflist, self.blocklist)[detail]
            for item in dao.execute(conn, dataset, block_name, data_tier_name, origin_site_name, logical_file_name, run_num,
                                 min_cdate, max_cdate, min_ldate, max_ldate, cdate,  ldate):
                yield item
    
    def listBlocksOrigin(self, origin_site_name="", dataset="", block_name=""):
        """
        This is the API to list all the blocks/datasets first generated in the site called origin_site_name,
        if origin_site_name is provided w/ no wildcards allow. If a fully spelled dataset is provided, then it will
        only list the blocks first generated from origin_site_name under the given dataset.
        """
        if not (dataset or block_name):
            dbsExceptionHandler("dbsException-invalid-input",
                                "DBSBlock/listBlocksOrigin: dataset or block_name must be provided.")
        if re.search("['%', '*']", dataset) or re.search("['%', '*']", block_name):
            dbsExceptionHandler("dbsException-invalid-input",
                                "DBSBlock/listBlocksOrigin: dataset or block_name with wildcard is not supported.")
        try:
            conn = self.dbi.connection()
            result = self.bkOriginlist.execute(conn, origin_site_name, dataset, block_name)
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
        if not ("block_name" in businput and "origin_site_name" in businput  ):
            dbsExceptionHandler('dbsException-invalid-input', "business/DBSBlock/insertBlock must have block_name and origin_site_name as input")
        conn = self.dbi.connection()
        tran = conn.begin()
        try:
            blkinput = {
                "last_modification_date":businput.get("last_modification_date",  dbsUtils().getTime()),
                #"last_modified_by":businput.get("last_modified_by", dbsUtils().getCreateBy()),
                "last_modified_by":dbsUtils().getCreateBy(),
                #"create_by":businput.get("create_by", dbsUtils().getCreateBy()),
                "create_by":dbsUtils().getCreateBy(),
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
        except Exception as e:
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
