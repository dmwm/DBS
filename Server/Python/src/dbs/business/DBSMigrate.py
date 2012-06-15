#!/usr/bin/env python
#pylint: disable=C0103
"""
This module provides dataset migration business object class. 
"""

__revision__ = "$Id: DBSMigrate.py,v 1.17 2010/09/14 14:53:54 yuyi Exp $"
__version__ = "$Revision: 1.17 $"

from WMCore.DAOFactory import DAOFactory

#temporary thing
import os, sys, socket
import json, cjson
import urllib, urllib2
import urlparse
import httplib
from dbs.utils.dbsUtils import dbsUtils
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.dbsException import dbsException, dbsExceptionCode
from dbs.utils.dbsHTTPSAuthHandler import HTTPSAuthHandler
from sqlalchemy import exceptions

def pprint(a):
    print json.dumps(a, sort_keys=True, indent=4)

class DBSMigrate:
    """ Migration business object class. """

    def __init__(self, logger, dbi, owner):
        
        daofactory = DAOFactory(package='dbs.dao', logger=logger,
                                dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        
        self.sm = daofactory(classname="SequenceManager")
        self.primdslist     = daofactory(classname="PrimaryDataset.List")
        self.datasetlist    = daofactory(classname="Dataset.List")
        self.filelist       = daofactory(classname="File.MgrtList") 
        self.fllist         = daofactory(classname="FileLumi.List")
        self.fplist         = daofactory(classname="FileParent.List")
        self.aelist         = daofactory(classname="AcquisitionEra.List")
        self.pelist         = daofactory(classname="ProcessingEra.List")
        self.mgrlist        = daofactory(classname="MigrationRequests.List")
        self.mgrin          = daofactory(classname="MigrationRequests.Insert")
        self.mgrup          = daofactory(classname="MigrationBlock.Update")
        self.mgrblkin       = daofactory(classname="MigrationBlock.Insert")
        self.blocklist      = daofactory(classname="Block.List")
        self.bparentlist    = daofactory(classname="BlockParent.List")
        self.dsparentlist   = daofactory(classname="DatasetParent.List")
        self.outputCoflist  = daofactory(classname="OutputModuleConfig.List")
        self.mgrremove      = daofactory(classname="MigrationRequests.Remove")


    def prepareDatasetMigrationList(self, conn, request):
        """
        Prepare the ordered lists of blocks based on input DATASET (note Block is different)
            1. Get list of blocks from source
            2. Check and see if these blocks are already at DST
            3. Check if dataset has parents
            4. Check if parent blocks are already at DST
            
        """
        ordered_dict = {}
        order_counter = 0 
        srcdataset = request["migration_input"]
        url = request["migration_url"]
        try:
            tmp_ordered_dict = self.processDatasetBlocks(url, conn,
                                            srcdataset, order_counter)
            if tmp_ordered_dict != {}:  
                ordered_dict.update(tmp_ordered_dict)
            else:
                return {}
            # Now process the parent datasets
            parent_ordered_dict = self.getParentDatastesOrderedList(url, conn,
                                                srcdataset, order_counter+1)
            if parent_ordered_dict != {}:
                ordered_dict.update(parent_ordered_dict)
            return ordered_dict  
        except Exception, ex:
            dbsExceptionHandler('dbsException-invalid-input2', \
                'DBSMigrate/prepareDatasetMigrationList failed\
                 to prepare ordered block list: %s' %str(ex))

    def processDatasetBlocks(self, url, conn, inputdataset, order_counter):
        """
        Utility function, that comapares blocks of a dataset at source and dst
        and returns an ordered list of blocks not already at dst for migration
        """
        ordered_dict = {}
        srcblks = self.getSrcBlocks(url, dataset=inputdataset)
        if len(srcblks) < 0:
            dbsExceptionHandler('dbsException-invalid-input2', 
                "Invalid input for DBSMigration: No blocks in the required dataset %s \
                found at source %s." % (inputdataset, url))
        dstblks = self.blocklist.execute(conn, dataset=inputdataset)
        blocksInSrcNames = [ y['block_name'] for y in srcblks]
        blocksInDstNames = [ x['block_name'] for x in dstblks]
        ordered_dict[order_counter] = []
        for ablk in blocksInSrcNames:
            if not ablk in blocksInDstNames:
                ordered_dict[order_counter].append(ablk)
        if ordered_dict[order_counter] != []: return ordered_dict
        else: return {}

    def getParentDatastesOrderedList(self, url, conn, dataset, order_counter):
        """
        check if input dataset has parents,
        check if any of the blocks are already at dst,
        prepare the ordered list and return it.
        url : source DBS url
        dataset : to be migrated dataset
        order_counter: the order in which migration happends. 
        """
        ordered_dict = {}
        parentSrcDatasets = self.getSrcDatasetParents(url, dataset)
        if len(parentSrcDatasets) > 0:
            parentSrcDatasetNames = [y['parent_dataset'] 
                                        for y in parentSrcDatasets]
            for aparentDataset in parentSrcDatasetNames:
                parent_ordered_dict = self.processDatasetBlocks(url, conn,
                                            aparentDataset, order_counter)
                if parent_ordered_dict != {}:
                    ordered_dict.update(parent_ordered_dict)
                # parents of parent
                pparent_ordered_dict = self.getParentDatastesOrderedList(url,
                                    conn, aparentDataset, order_counter+1)
                if pparent_ordered_dict != {}:
                    ordered_dict.update(pparent_ordered_dict)
        return ordered_dict

    def prepareBlockMigrationList(self, conn, request):
        """
        Prepare the ordered lists of blocks based on input BLOCK
            1. see if block already exists at dst (no need to migrate),
               raise "ALREADY EXISTS"
            2. see if block exists at src
            3. see if block has parents
            4. see if parent blocks are already at dst
            5. add 'order' to parent and then this block (ascending)
            6. return the ordered list
        """
        ordered_dict = {}
        block_name = request["migration_input"]
        url = request["migration_url"]
        order_counter = 0
        try:
            #1.
            dstblock = self.blocklist.execute(conn, block_name=block_name)
            if len(dstblock) > 0:
                dbsExceptionHandler('dbsException-invalid-input', 'ALREADY EXISTS: \
                    Required block (%s) migration is already at destination' %block_name)
            #2.
            srcblock = self.getSrcBlocks(url, block=block_name)
            if len(srcblock) < 1:
                dbsExceptionHandler('dbsException-invalid-input2', '''Invalid input for DBSMigration:
                                       Required Block %s not found at source %s. ''' %(block, url))
            ##This block has to be migrated
            ordered_dict[order_counter] = []
            ordered_dict[order_counter].append(block_name)
            parent_ordered_dict = self.getParentBlocksOrderedList(url, conn,
                                                block_name, order_counter+1)
            if parent_ordered_dict != {}:
                ordered_dict.update(parent_ordered_dict)
            #6.
            return ordered_dict
        except Exception, ex:
            raise ex
 
    def getParentBlocksOrderedList(self, url, conn, block_name, order_counter):
        ordered_dict = {}
        #3.
        parentBlocksInSrc = self.getSrcBlockParents(url, block_name)
        parentBlocksInSrcNames = [ y['parent_block_name'] 
                                        for y in parentBlocksInSrc ]
        #4.
        if len(parentBlocksInSrcNames) > 0:
            ordered_dict[order_counter] = []
            # check which of these are already at dst
            # the only way we can do, is to list blocks for parent dataset,
            # and then just check the ones we are interested in.
            # This assumes that all parent blocks are from a same dataset. Is this true? YG 6/12/2012
            # Confirmed by S. Foulkes, all parent's blocks belongs to a same dataset. 
            parent_dataset = parentBlocksInSrcNames[0].split('#')[0]
            parentBlocksInDst = self.blocklist.execute(conn, parent_dataset)
            parentBlocksInDstNames = [y['parent_block_name']
                                            for y in parentBlocksInDst]
            for ablockAtSrc in parentBlocksInSrcNames:
                if ablockAtSrc not in parentBlocksInDstNames:
                    #block is not already at dst
                    #5.
                    ordered_dict[order_counter].append(ablockAtSrc)
                    #Also check if it has parents (recurrsion begins)
                    tmp_ordered_dict = self.getParentBlocksOrderedList(url,
                                            conn, ablockAtSrc, order_counter+1)
                    if tmp_ordered_dict != {}:
                        ordered_dict[order_counter+1] = []
                        ordered_dict.update(tmp_ordered_dict)
        return ordered_dict

    def removeMigrationRequest(self, migration_rqst_id):
        """
        Method to remove pending or failed migration request from the queue.

        """
        conn = self.dbi.connection()
        try:
            tran = conn.begin()
            self.mgrremove.execute(conn, migration_rqst_id)  
            tran.commit()
        except Exception, ex:
            if conn: conn.close()
            raise
        if conn: conn.close()


                        
    def insertMigrationRequest(self, request):
        """
        Method to insert use requests to MIGRATION_REQUESTS table.
        request keys: migration_url, migration_input
        """
    
        conn = self.dbi.connection()
        # check if already queued.
        #If the migration_input is the same, but the src url is different,
        #We will consider it as a submitted request. YG 05-18-2012
        try:
            alreadyqueued = self.mgrlist.execute(conn, 
                    migration_input=request["migration_input"])
            #if the queued is not failed, then we don't need to do it again.
            if len(alreadyqueued) > 0 and alreadyqueued[0]['migration_status'] != 3:
                return {"migration_report" : "REQUEST ALREADY QUEUED",
                        "migration_details" : alreadyqueued[0] }
            # not already queued            
            #Determine if its a dataset or block migration
            #The prepare list calls will check if the requested blocks/dataset already in destination.
            if request["migration_input"].find("#") != -1:
                ordered_list = self.prepareBlockMigrationList(conn, request)
            else:
                ordered_list = self.prepareDatasetMigrationList(conn, request)
            # now we have the blocks that need to be queued (ordered)
        except Exception, ex:
            if conn: conn.close()
            raise
            
        tran = conn.begin()
        try:
            # insert the request
            #request.update(migration_status=0)
            request['migration_request_id'] = self.sm.increment(conn, "SEQ_MR", tran)
            self.mgrin.execute(conn, request, tran)
            # INSERT the ordered_list
            totalQueued = 0
            for iter in reversed(range(len(ordered_list))):
                if len(ordered_list[iter]) > 0:
                    daoinput = [{
                        "migration_block_id" : 
                            self.sm.increment(conn, "SEQ_MB", tran),
                        "migration_request_id" : 
                            request["migration_request_id"],
                        "migration_block_name" : blk,
                        "migration_order" : iter,
                        "migration_status" : 0,
                        "creation_date" : request['creation_date'],
                        "last_modification_date" : request['last_modification_date'],
                        "create_by" : request['create_by'],
                        "last_modified_by" : request['last_modified_by'] }
                             for blk in ordered_list[iter]]
                    self.mgrblkin.execute(conn, daoinput, tran) 
                    totalQueued += len(ordered_list[iter])
            # all good ?, commit the transaction
            tran.commit()
            if conn: conn.close()
            # return things like (X blocks queued for migration)
            return {
                "migration_report" : "REQUEST QUEUED with total %d blocks to be migrated" %totalQueued,
                "migration_details" : request }
        except exceptions.IntegrityError, ex:
            tran.rollback()
            if conn: conn.close()
            if (str(ex).find("unique constraint") != -1 or
                str(ex).lower().find("duplicate") != -1):
                #The unique constraints are: MIGRATION_REQUESTS(MIGRATION_INPUT)
                #MIGRATION_BLOCKS(MIGRATION_BLOCK_NAME, MIGRATION_REQUEST_ID)
                return {
                    "migration_report" : "REQUEST ALREADY QUEUED",
                    "migration_details" : request }
            else:
                if conn: conn.close()
                dbsExceptionHandler('dbsException-invalid-input2',"DBSMigration:  ENQUEUEING_FAILED; reason may be (%s)" %ex)
        except Exception, ex:
            if tran: tran.rollback()
            if conn: conn.close()
            dbsExceptionHandler('dbsException-invalid-input2',"DBSMigration:  ENQUEUEING_FAILED; reason may be (%s)" %ex)
        finally:
            if conn: conn.close()
    
    def listMigrationRequests(self, migration_request_id="", block_name="",
                              dataset="", user=""):
        """
        get the status of the migration
        migratee : can be dataset or block_name
        """
        
        conn = self.dbi.connection()
        migratee = ""
        try:
            if block_name:
                migratee = block_name
            elif dataset:
                migratee = dataset
            result = self.mgrlist.execute(conn, migration_url="",
                    migration_input=migratee, create_by=user,
                    migration_request_id=migration_request_id)
            return result
        finally:
            if conn: conn.close()

    def updateMigrationBlockStatus(self, migration_status, migration_block):
        """
        migration_status: 
        0=PENDING
        1=IN PROGRESS
        2=COMPLETED
        3=FAILED
        status change: 
        0 -> 1
        1 -> 2
        1 -> 3
        are only allowed changes.

        """
        
        conn = self.dbi.connection()
        try:
            upst = dict(migration_status=migration_status, 
                        migration_block_name=migration_block)
            self.mgrup.execute(conn, upst)
        finally:
            if conn:conn.close()

    ##-- below are the actual migration methods

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
            if dataset1: dataset = dataset1[0]

            #get block parentage
            bparent = self.bparentlist.execute(conn, block['block_name'])
            #get dataset parentage
            dsparent = self.dsparentlist.execute(conn, dataset['dataset'])
            for p in dsparent:
                del p['parent_dataset_id'], p['dataset']
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
                            logical_file_name=f['logical_file_name']))
                del f['branch_hash_id']
            del dataset["acquisition_era_name"], dataset["processing_version"]
            del block["dataset"]
            result = dict(block=block, dataset=dataset, primds=primds,
                          files=files, block_parent_list=bparent,
                          ds_parent_list=dsparent, file_conf_list=fconfig_list,
                          file_parent_list=fparent_list)
            if acqEra:
                result["acquisition_era"] = acqEra
            if prsEra:
                result["processing_era"] = prsEra
            return result
        finally:
            if conn:
                conn.close()
        
    def callDBSService(self, resturl):
        try:
            spliturl = urlparse.urlparse(resturl)
            callType = spliturl[0]
            if callType == 'http':
                opener =  urllib2.build_opener()
            elif callType == 'https':
                key1, cert1 = self.__getKeyCert()
                debug=0
                https_handler  = HTTPSAuthHandler(key1, cert1, debug)
                opener = urllib2.build_opener(https_handler)
            else:
                raise ValueError, "unknown URL type: %s" % callType
        
            req = urllib2.Request(url = resturl)
            data = opener.open(req)
            ddata = cjson.decode(data.read())
            data.close()
            return ddata
        except urllib2.HTTPError, httperror:
            raise httperror
        except urllib2.URLError, urlerror:
            raise urlerror
        except Exception, e:
            raise e
   
    def __getKeyCert(self):
        """
        Get the user credentials if they exist, otherwise throw an exception.
        
        This code was modified from DBSAPI/dbsHttpService.py and WMCore/Services/Requests.py

        """
        # Zeroth case is if the class has over ridden the key/cert and has it
        # stored in self
        if getattr(self, 'cert', None) and getattr(self, 'key', None):
            key = self.key
            cert = self.cert

        # Now we're trying to guess what the right cert/key combo is...
        # First preference to HOST Certificate, This is how it set in Tier0
        elif os.environ.has_key('X509_HOST_CERT'):
            cert = os.environ['X509_HOST_CERT']
            key = os.environ['X509_HOST_KEY']

        # Second preference to User Proxy, very common
        elif (os.environ.has_key('X509_USER_PROXY')) and \
                (os.path.exists( os.environ['X509_USER_PROXY'])):
            cert = os.environ['X509_USER_PROXY']
            key = cert

        # Third preference to User Cert/Proxy combinition
        elif os.environ.has_key('X509_USER_CERT'):
            cert = os.environ['X509_USER_CERT']
            key = os.environ['X509_USER_KEY']

        # TODO: only in linux, unix case, add other os case
        # look for proxy at default location /tmp/x509up_u$uid
        elif os.path.exists('/tmp/x509up_u'+str(os.getuid())):
            cert = '/tmp/x509up_u'+str(os.getuid())
            key = cert

        elif sys.stdin.isatty():
            if os.path.exists(os.environ['HOME'] + '/.globus/usercert.pem'):
                cert = os.environ['HOME'] + '/.globus/usercert.pem'
                if os.path.exists(os.environ['HOME'] + '/.globus/userkey.pem'):
                    key = os.environ['HOME'] + '/.globus/userkey.pem'
                else:
                    key = cert

        else:
            raise dbsClientException("auth-error","No valid X509 cert-key-pair found.")
        #Set but not found
        if  os.path.isfile(key) and  os.path.isfile(cert):
            return key, cert
        else:
            raise ValueError, "key or cert file does not exist: %s, %s" % (key,cert)

 
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
        blockname = block.replace("#", urllib.quote_plus('#'))
        resturl = "%s/blockparents?block_name=%s" % (url, blockname)
        return self.callDBSService(resturl)
    
    def getSrcBlocks(self, url, dataset="", block=""):
        """
        Need to list all blocks of the dataset and its parents starting from the top
        For now just list the blocks from this dataset.
        Client type call...
        """
        if block:
            blockname = block.replace("#", urllib.quote_plus('#'))
            resturl = "%s/blocks?block_name=%s" % (url, blockname)
        elif dataset:
            resturl = "%s/blocks?dataset=%s" % (url, dataset)
        else:
            dbsExceptionHandler('dbsException-invalid-input2', 'Invalid inputs for\
                DBSMigrate/getSrcBlocks. Either block or dataset name has to be\
                provided.')
        return self.callDBSService(resturl)
