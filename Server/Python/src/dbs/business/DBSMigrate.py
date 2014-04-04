#!/usr/bin/env python
#pylint: disable=C0103
"""
This module provides dataset migration business object class.
"""

__revision__ = "$Id: DBSMigrate.py,v 1.17 2010/09/14 14:53:54 yuyi Exp $"
__version__ = "$Revision: 1.17 $"

from WMCore.DAOFactory import DAOFactory

#temporary thing
import cjson
import json
import os
import socket
import urllib2
import urlparse

from dbs.utils.dbsUtils import dbsUtils
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.dbsException import dbsException, dbsExceptionCode
from dbs.utils.RestClientPool import RestClientPool

from RestClient.ErrorHandling.RestClientExceptions import HTTPError

from sqlalchemy import exceptions

def pprint(a):
    print json.dumps(a, sort_keys=True, indent=4)

def remove_duplicated_items(ordered_dict):
    unique_block_list = set()

    for key, value in reversed(ordered_dict.items()):
        for entry in list(value):#copy the list since value is modified during iteration
            if entry not in unique_block_list:
                unique_block_list.add(entry)
            else:
                value.remove(entry)
    return ordered_dict

class DBSMigrate:
    """ Migration business object class. """

    def __init__(self, logger, dbi, owner):

        daofactory = DAOFactory(package='dbs.dao', logger=logger,
                                dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi

        myproxy = os.environ.get('SOCKS5_PROXY', None)
        self.rest_client_pool = RestClientPool(proxy=myproxy)

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
        self.mgrRqUp        = daofactory(classname="MigrationRequests.UpdateRequestStatus")
        self.mgrup          = daofactory(classname="MigrationBlock.Update")
        self.mgrblkin       = daofactory(classname="MigrationBlock.Insert")
        self.mgrblklist     = daofactory(classname="MigrationBlock.List")
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
                #return {}
                m = 'Requested dataset %s is already in destination' %srcdataset
                dbsExceptionHandler('dbsException-invalid-input2', message=m, serverError=m)
            # Now process the parent datasets
            parent_ordered_dict = self.getParentDatasetsOrderedList(url, conn,
                                                srcdataset, order_counter+1)
            if parent_ordered_dict != {}:
                ordered_dict.update(parent_ordered_dict)
            return remove_duplicated_items(ordered_dict)
        except dbsException:
            raise
        except Exception, ex:
            if 'urlopen error' in str(ex):
                message='Connection to source DBS server refused. Check your source url.'
            elif 'Bad Request' in str(ex):
                message='cannot get data from the source DBS server. Check your migration input.'
            else:
                message='Failed to make a dataset migration list.'
            dbsExceptionHandler('dbsException-invalid-input2', \
                serverError="""DBSMigrate/prepareDatasetMigrationList failed
                to prepare ordered block list: %s""" %str(ex), message=message)

    def processDatasetBlocks(self, url, conn, inputdataset, order_counter):
        """
        Utility function, that comapares blocks of a dataset at source and dst
        and returns an ordered list of blocks not already at dst for migration
        """
        ordered_dict = {}
        srcblks = self.getSrcBlocks(url, dataset=inputdataset)
        if len(srcblks) < 0:
            e = "DBSMigration: No blocks in the required dataset %s found at source %s."%(inputdataset, url)
            dbsExceptionHandler('dbsException-invalid-input2', message=e, serverError=e)
        dstblks = self.blocklist.execute(conn, dataset=inputdataset)
        blocksInSrcNames = [ y['block_name'] for y in srcblks]
        blocksInDstNames = [ x['block_name'] for x in dstblks]
        ordered_dict[order_counter] = []
        for ablk in blocksInSrcNames:
            if not ablk in blocksInDstNames:
                ordered_dict[order_counter].append(ablk)
        if ordered_dict[order_counter] != []:
            return ordered_dict
        else:
            return {}

    def getParentDatasetsOrderedList(self, url, conn, dataset, order_counter):
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
                pparent_ordered_dict = self.getParentDatasetsOrderedList(url,
                                    conn, aparentDataset, order_counter+1)
                if pparent_ordered_dict != {}:
                    ordered_dict.update(pparent_ordered_dict)
        return ordered_dict

    def prepareBlockMigrationList(self, conn, request):
        """
        Prepare the ordered lists of blocks based on input BLOCK
            1. see if block already exists at dst (no need to migrate),
               raise "ALREADY EXISTS"
            2. see if block exists at src & make sure the block's open_for_writting=0
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
                e = 'DBSMigration: Invalid input. Required Block %s not found at source %s.' %(srcblock, url)
                dbsExceptionHandler('dbsException-invalid-input2', message=e, serverError=e)
            ##This block has to be migrated
            ordered_dict[order_counter] = []
            ordered_dict[order_counter].append(block_name)
            parent_ordered_dict = self.getParentBlocksOrderedList(url, conn,
                                                block_name, order_counter+1)
            if parent_ordered_dict != {}:
                ordered_dict.update(parent_ordered_dict)
            #6.
            #check for duplicates

            return remove_duplicated_items(ordered_dict)
        except Exception, ex:
	    if 'urlopen error' in str(ex):
                message='Connection to source DBS server refused. Check your source url.'
            elif 'Bad Request' in str(ex):
                message='cannot get data from the source DBS server. Check your migration input.'
            else:
                message='Failed to make a block migration list.'
            dbsExceptionHandler('dbsException-invalid-input2', \
                serverError="""DBSMigrate/prepareBlockMigrationList failed
                to prepare ordered block list: %s""" %str(ex), message=message)

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
            # 3/6/2014 YG: Seems it is not the case that all parents are from the same dataset regarding to crab data
	    parent_ds_lst = []	
	    for ps in parentBlocksInSrcNames:	
		parent_dataset = ps.split('#')[0]
		if parent_dataset not in parent_ds_lst:
			parent_ds_lst.append(parent_dataset)
	    parentBlocksInDst = []
 	    for rpds in parent_ds_lst:
		lpds =  self.blocklist.execute(conn, rpds)
		parentBlocksInDst.extend(lpds)
            #YG 7/24/2012
            parentBlocksInDstNames = [y['block_name']
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
                        #ordered_dict[order_counter+1] = []
                        #ordered_dict.update(tmp_ordered_dict)
                        for i in tmp_ordered_dict.keys():
                                if i in ordered_dict.keys():
                                    ordered_dict[i] += tmp_ordered_dict[i]
                                else:
                                    ordered_dict[i] = tmp_ordered_dict[i]
        return ordered_dict

    def removeMigrationRequest(self, migration_rqst):
        """
        Method to remove pending or failed migration request from the queue.

        """
        conn = self.dbi.connection()
        try:
            tran = conn.begin()
            self.mgrremove.execute(conn, migration_rqst)
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
            is_already_queued = len(alreadyqueued) > 0
            # close connection before returning json object
            if is_already_queued and conn:
                conn.close()
            #if the queued is not failed, then we don't need to do it again.
            #add a new migration_status=9 (terminal failure)
	    if is_already_queued and alreadyqueued[0]['migration_status'] == 2:
                return {"migration_report" : "REQUEST ALREADY QUEUED. Migration is finished",
                        "migration_details" : alreadyqueued[0] }
            elif is_already_queued and alreadyqueued[0]['migration_status'] != 9:
                return {"migration_report" : "REQUEST ALREADY QUEUED. Migration in progress",
                        "migration_details" : alreadyqueued[0] }
            elif is_already_queued and alreadyqueued[0]['migration_status'] == 9:
                return {"migration_report" : "REQUEST ALREADY QUEUED. Migration terminally failed. ",
                        "migration_details" : alreadyqueued[0] }
            else:
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
                        "last_modified_by" : request['last_modified_by']
                        }
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
                #FIXME: Need to check which unique key. YG 2/11/13
                #The unique constraints are: MIGRATION_REQUESTS(MIGRATION_INPUT)
                #MIGRATION_BLOCKS(MIGRATION_BLOCK_NAME, MIGRATION_REQUEST_ID)
                return {
                    "migration_report" : "REQUEST ALREADY QUEUED",
                    "migration_details" : request }
            else:
                if conn: conn.close()
                m = "DBSMigration:  ENQUEUEING_FAILED."
                e = "DBSMigration:  ENQUEUEING_FAILED; reason may be (%s)" %str(ex)
                dbsExceptionHandler('dbsException-invalid-input2', message=m, serverError=e)
        except Exception, ex:
            if tran: tran.rollback()
            if conn: conn.close()
            m = "DBSMigration:  ENQUEUEING_FAILED."
            e = "DBSMigration:  ENQUEUEING_FAILED; reason may be (%s)" %str(ex)
            dbsExceptionHandler('dbsException-invalid-input2',message=m, serverError=e)
        finally:
            if conn: conn.close()

    def listMigrationRequests(self, migration_request_id="", block_name="",
                              dataset="", user="", oldest=False):
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
                    migration_request_id=migration_request_id, oldest=oldest)
            return result

        finally:
            if conn: conn.close()

    def listMigrationBlocks(self, migration_request_id=""):
        """
        get eveything of block that is has status = 0 and migration_request_id as specified.
        """

        conn = self.dbi.connection()
        try:
            return self.mgrblklist.execute(conn, migration_request_id=migration_request_id)
        finally:
            if conn: conn.close()


    def updateMigrationRequestStatus(self, migration_status, migration_request_id):
        """
        migration_status:
        0=PENDING
        1=IN PROGRESS
        2=COMPLETED
        3=FAILED (will be retried)
        9=Terminally FAILED 
        status change:
        0 -> 1
        1 -> 2
        1 -> 3
        1 -> 9
        are only allowed changes for working through migration.
        3 -> 1 is allowed for retrying and retry count +1.

        """

        conn = self.dbi.connection()
        tran = conn.begin()
        try:
            upst = dict(migration_status=migration_status,
                        migration_request_id=migration_request_id,
                        last_modification_date=dbsUtils().getTime())
            self.mgrRqUp.execute(conn, upst)
        except:
            if tran:tran.rollback()
            raise
        else:
            if tran:tran.commit()
        finally:
            #open transaction is committed when conn closed.
            if conn:conn.close()

    ##-- below are the actual migration methods



    def updateMigrationBlockStatus(self, migration_status=0, migration_block=None, migration_request=None):
        """
        migration_status:
        0=PENDING
        1=IN PROGRESS
        2=COMPLETED
        3=FAILED (will be retried)
        9=Terminally FAILED
        status change:
        0 -> 1
        1 -> 2
        1 -> 3
        1 -> 9
        are only allowed changes for working through migration.
        3 -> 1 allowed for retrying.

        """

        conn = self.dbi.connection()
        tran = conn.begin()
        try:
            if migration_block:
                upst = dict(migration_status=migration_status,
                        migration_block_id=migration_block,last_modification_date=dbsUtils().getTime())
            elif migration_request:
                upst = dict(migration_status=migration_status, migration_request_id=migration_request,
                            last_modification_date=dbsUtils().getTime())
            self.mgrup.execute(conn, upst)
        except:
            if tran:tran.rollback()
            raise
        else:
            if tran:tran.commit()
        finally:
            if conn:conn.close()

    ##-- below are the actual migration methods

    def callDBSService(self, resturl, method='', params={}, data={}):
        try:
            spliturl = urlparse.urlparse(resturl)
            callType = spliturl[0]
            if callType != 'http' and callType != 'https':
                raise ValueError, "unknown URL type: %s" % callType

            content = "application/json"
            UserID = os.environ['USER']+'@'+socket.gethostname()
            request_headers =  {"Content-Type": content, "Accept": content, "UserID": UserID }
            #params = {'block_name':blockname}
            data = cjson.encode(data)
            restapi = self.rest_client_pool.get_rest_client()
            httpresponse = restapi.get(resturl, method, params, data, request_headers)
            return httpresponse.body
        except urllib2.HTTPError, httperror:
            raise httperror
        except urllib2.URLError, urlerror:
            raise urlerror
        except HTTPError, DBShttp_error:
            raise DBShttp_error
        except Exception, e:
            raise e

    def getSrcDatasetParents(self, url, dataset):
        """
        List block at src DBS
        """
        #resturl = "%s/datasetparents?dataset=%s" % (url, dataset)
        params={'dataset':dataset}
        return cjson.decode(self.callDBSService(url, 'datasetparents', params,{}))

    def getSrcBlockParents(self, url, block):
        """
        List block at src DBS
        """
        #blockname = block.replace("#", urllib.quote_plus('#'))
        #resturl = "%s/blockparents?block_name=%s" % (url, blockname)
        params={'block_name':block}
        return cjson.decode(self.callDBSService(url, 'blockparents', params, {}))

    def getSrcBlocks(self, url, dataset="", block=""):
        """
        Need to list all blocks of the dataset and its parents starting from the top
        For now just list the blocks from this dataset.
        Client type call...
        """
        if block:
            params={'block_name':block, 'open_for_writting':0}
        elif dataset:
            params={'dataset':dataset, 'open_for_writting':0}
        else:
            m = 'DBSMigration: Invalid input.  Either block or dataset name has to be provided'
            e = 'DBSMigrate/getSrcBlocks: Invalid input.  Either block or dataset name has to be provided'
            dbsExceptionHandler('dbsException-invalid-input2', message=m, serverError=e )

        return cjson.decode(self.callDBSService(url, 'blocks', params, {}))
