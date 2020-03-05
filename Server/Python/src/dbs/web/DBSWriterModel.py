#!/usr/bin/env python
#pylint: disable=C0103
"""
DBS Rest Model module
"""
import re
import cjson

from cherrypy import request, tools, HTTPError
from WMCore.DAOFactory import DAOFactory
from dbs.utils.dbsUtils import dbsUtils
from dbs.web.DBSReaderModel import DBSReaderModel
from dbs.web.DBSReaderModel import authInsert
from dbs.utils.dbsException import dbsException, dbsExceptionCode
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.DBSInputValidation import *
from dbs.utils.DBSTransformInputType import transformInputType

import traceback

# CMSMonitoring modules
from CMSMonitoring.NATS import NATSManager


class DBSWriterModel(DBSReaderModel):
    """
    DBS3 Server API Documentation
    """
    def __init__(self, config):
        """
        All parameters are provided through DBSConfig module
        """

        #Dictionary with reader and writer as keys
        urls = config.database.connectUrl

        #instantiate the page with the writer_config

        if isinstance(urls, type({})):
            config.database.connectUrl = urls['writer']

        DBSReaderModel.__init__(self, config)

        # set proper logger name
        self.logger.name = __name__

        # initialize NATS if requested
        self.nats = None
        if hasattr(config, 'use_nats') and config.use_nats:
            topic = 'cms.dbs'
            topics = config.nats_topics
            if not topics:
                topics = ['%s.topic' % topic]
            self.nats = NATSManager(config.nats_server, topics=topics, default_topic=topic)
            msg = "DBS NATS: %s" % self.nats
            self.logger.info(msg)

        self.sequenceManagerDAO = self.daofactory(classname="SequenceManager")
        self.dbsDataTierInsertDAO = self.daofactory(classname="DataTier.Insert")

        self._addMethod('POST', 'primarydatasets', self.insertPrimaryDataset, secured=True,
                         security_params={'role':self.security_params, 'authzfunc':authInsert}, dump_request_info=True)
        self._addMethod('POST', 'outputconfigs', self.insertOutputConfig,  secured=True,
                         security_params={'role':self.security_params, 'authzfunc':authInsert}, dump_request_info=True)
        self._addMethod('POST', 'acquisitioneras', self.insertAcquisitionEra, secured=True,
                         security_params={'role':self.security_params, 'authzfunc':authInsert}, dump_request_info=True)
        self._addMethod('PUT', 'acquisitioneras', self.updateAcqEraEndDate, args=['acquisition_era_name', 'end_date'],
                         secured=True, security_params={'role':self.security_params, 'authzfunc':authInsert}, dump_request_info=True)
        self._addMethod('POST', 'processingeras', self.insertProcessingEra, secured=True,
                         security_params={'role':self.security_params, 'authzfunc':authInsert}, dump_request_info=True)
        self._addMethod('POST', 'datasets', self.insertDataset, secured=True,
                        security_params={'role':self.security_params, 'authzfunc':authInsert}, dump_request_info=True)
        self._addMethod('POST', 'blocks', self.insertBlock, secured=True,
                         security_params={'role':self.security_params, 'authzfunc':authInsert}, dump_request_info=True)
        self._addMethod('POST', 'files', self.insertFile, args=['qInserts'], secured=True,
                         security_params={'role':self.security_params, 'authzfunc':authInsert}, dump_request_info=True)
        self._addMethod('PUT', 'files', self.updateFile, args=['logical_file_name', 'is_file_valid', 'lost', 'dataset'],
                         secured=True, security_params={'role':self.security_params, 'authzfunc':authInsert}, dump_request_info=True)
        self._addMethod('PUT', 'datasets', self.updateDataset, args=['dataset', 'dataset_access_type'],
                         secured=True, security_params={'role':self.security_params, 'authzfunc':authInsert}, dump_request_info=True)
        self._addMethod('PUT', 'blocks', self.updateBlock, args=['block_name', 'open_for_writing', 'origin_site_name'],
                         secured=True, security_params={'role':self.security_params, 'authzfunc':authInsert}, dump_request_info=True)
        self._addMethod('POST', 'datatiers', self.insertDataTier, secured=True,
                         security_params={'role':self.security_params, 'authzfunc':authInsert}, dump_request_info=True)
        self._addMethod('POST', 'bulkblocks', self.insertBulkBlock, secured=True,
                         security_params={'role':self.security_params, 'authzfunc':authInsert}, dump_request_info=True)
        self._addMethod('POST', 'fileparents', self.insertFileParents, secured=True,
                         security_params={'role':self.security_params, 'authzfunc':authInsert}, dump_request_info=True)

    def insertPrimaryDataset(self):
        """
        API to insert A primary dataset in DBS

        :param primaryDSObj: primary dataset object
        :type primaryDSObj: dict
        :key primary_ds_type: TYPE (out of valid types in DBS, MC, DATA) (Required)
        :key primary_ds_name: Name of the primary dataset (Required)

        """
        try :
            body = request.body.read()
            indata = cjson.decode(body)
            indata = validateJSONInputNoCopy("primds", indata)
            indata.update({"creation_date": dbsUtils().getTime(), "create_by": dbsUtils().getCreateBy() })
            self.dbsPrimaryDataset.insertPrimaryDataset(indata)
        except cjson.DecodeError as dc:
            dbsExceptionHandler("dbsException-invalid-input2", "Wrong format/data from insert PrimaryDataset input",  self.logger.exception, str(dc))
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except HTTPError as he:
            raise he
        except Exception as ex:
            sError = "DBSWriterModel/insertPrimaryDataset. %s\n Exception trace: \n %s" \
                        % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    def insertOutputConfig(self):
        """
        API to insert An OutputConfig in DBS

        :param outputConfigObj: Output Config object
        :type outputConfigObj: dict
        :key app_name: App Name (Required)
        :key release_version: Release Version (Required)
        :key pset_hash: Pset Hash (Required)
        :key output_module_label: Output Module Label (Required)
        :key global_tag: Global Tag (Required)
        :key scenario: Scenario (Optional, default is None)
        :key pset_name: Pset Name (Optional, default is None)

        """
        try:
            body = request.body.read()
            indata = cjson.decode(body)
            indata = validateJSONInputNoCopy("dataset_conf_list", indata)
            indata.update({"creation_date": dbsUtils().getTime(),
                           "create_by" : dbsUtils().getCreateBy()})
            self.dbsOutputConfig.insertOutputConfig(indata)
        except cjson.DecodeError as dc:
            dbsExceptionHandler("dbsException-invalid-input2", "Wrong format/data from insert OutputConfig input",  self.logger.exception, str(dc))
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except HTTPError as he:
            raise he
        except Exception as ex:
            sError = "DBSWriterModel/insertOutputConfig. %s\n. Exception trace: \n %s" \
                            % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(acquisition_era_name=basestring, end_date=(int, basestring))
    def updateAcqEraEndDate(self, acquisition_era_name ="", end_date=0):
        """
        API to update the end_date of an acquisition era

        :param acquisition_era_name: acquisition_era_name to update (Required)
        :type acquisition_era_name: str
        :param end_date: end_date not zero (Required)
        :type end_date: int

        """
        try:
            self.dbsAcqEra.UpdateAcqEraEndDate( acquisition_era_name, end_date)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except HTTPError as he:
            raise he
        except Exception as ex:
            sError = "DBSWriterModel/update.AcqEraEndDate %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    def insertAcquisitionEra(self):
        """
        API to insert an Acquisition Era in DBS

        :param acqEraObj: Acquisition Era object
        :type acqEraObj: dict
        :key acquisition_era_name: Acquisition Era Name (Required)
        :key start_date: start date of the acquisition era (unixtime, int) (Optional, default current date)
        :key end_date: end data of the acquisition era (unixtime, int) (Optional)

        """
        try:
            body = request.body.read()
            indata = cjson.decode(body)
            indata = validateJSONInputNoCopy("acquisition_era", indata)
            indata.update({"start_date": indata.get("start_date", dbsUtils().getTime()),\
                           "creation_date": indata.get("creation_date", dbsUtils().getTime()), \
                           "create_by" : dbsUtils().getCreateBy() })
            self.dbsAcqEra.insertAcquisitionEra(indata)
        except cjson.DecodeError as dc:
            dbsExceptionHandler("dbsException-invalid-input2", "Wrong format/data from insert AcquisitionEra input",  self.logger.exception, str(dc))
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except HTTPError as he:
            raise he
        except Exception as ex:
            sError = " DBSWriterModel/insertAcquisitionEra. %s\n. Exception trace: \n %s" \
                        % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    def insertProcessingEra(self):
        """
        API to insert A Processing Era in DBS

        :param procEraObj: Processing Era object
        :type procEraObj: dict
        :key processing_version: Processing Version (Required)
        :key description: Description (Optional)

        """
        try:
            body = request.body.read()
            indata = cjson.decode(body)
            indata = validateJSONInputNoCopy('processing_era', indata)
            indata.update({"creation_date": indata.get("creation_date", dbsUtils().getTime()), \
                           "create_by" : dbsUtils().getCreateBy() })
            self.dbsProcEra.insertProcessingEra(indata)
        except cjson.DecodeError as dc:
            dbsExceptionHandler("dbsException-invalid-input2", "Wrong format/data from insert ProcessingEra input",  self.logger.exception, str(dc))
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except HTTPError as he:
            raise he
        except Exception as ex:
            sError = "DBSWriterModel/insertProcessingEra. %s\n. Exception trace: \n %s" \
                            % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    def insertDataset(self):
        """
        API to insert a dataset in DBS

        :param datasetObj: Dataset object
        :type datasetObj: dict
        :key primary_ds_name: Primary Dataset Name (Required)
        :key dataset: Name of the dataset (Required)
        :key dataset_access_type: Dataset Access Type (Required)
        :key processed_ds_name: Processed Dataset Name (Required)
        :key data_tier_name: Data Tier Name (Required)
        :key acquisition_era_name: Acquisition Era Name (Required)
        :key processing_version: Processing Version (Required)
        :key physics_group_name: Physics Group Name (Optional, default None)
        :key prep_id: ID of the Production and Reprocessing management tool (Optional, default None)
        :key xtcrosssection: Xtcrosssection (Optional, default None)
        :key output_configs: List(dict) with keys release_version, pset_hash, app_name, output_module_label and global tag

        """
        try:
            body = request.body.read()
            indata = cjson.decode(body)
            indata = validateJSONInputNoCopy('dataset', indata)
            indata.update({"creation_date": dbsUtils().getTime(),
                            "last_modification_date" : dbsUtils().getTime(),
                            "create_by" : dbsUtils().getCreateBy() ,
                            "last_modified_by" : dbsUtils().getCreateBy() })

            # need proper validation
            self.dbsDataset.insertDataset(indata)
            # send message to NATS if it is configured
            if self.nats:
                try:
                    dataset = indata.get('dataset')
                    dataset_access_type = indata.get('dataset_access_type')
                    doc = {'dataset':dataset, 'dataset_type': dataset_access_type}
                    self.nats.publish(doc)
                except Exception as exp:
                    err = 'insertDataset NATS error, %s, trace:\n%s' % (str(exp), traceback.format_exc())
                    self.logger.warning(err)
        except cjson.DecodeError as dc:
            dbsExceptionHandler("dbsException-invalid-input2", "Wrong format/data from insert dataset input",  self.logger.exception, str(dc)) 
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except HTTPError as he:
            raise he
        except Exception as ex:
            sError = " DBSWriterModel/insertDataset. %s\n. Exception trace: \n %s" \
                        % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    def insertBulkBlock(self):
        """
        API to insert a bulk block

        :param blockDump: Output of the block dump command
        :type blockDump: dict

        """
        try:
            body = request.body.read()
            indata = cjson.decode(body)
            if (indata.get("file_parent_list", []) and indata.get("dataset_parent_list", [])): 
                dbsExceptionHandler("dbsException-invalid-input2", "insertBulkBlock: dataset and file parentages cannot be in the input at the same time",  
                    self.logger.exception, "insertBulkBlock: datset and file parentages cannot be in the input at the same time.")    
            indata = validateJSONInputNoCopy("blockBulk", indata)
            self.dbsBlockInsert.putBlock(indata)
            # send message to NATS if it is configured
            if self.nats:
                try:
                    ddata = indata.get('dataset')
                    if isinstance(ddata, dict) and 'dataset' in ddata:
                        dataset = ddata.get('dataset')
                        dataset_access_type = ddata.get('dataset_access_type')
                        doc = {'dataset':dataset, 'dataset_type': dataset_access_type}
                        self.nats.publish(doc)
                except Exception as exp:
                    err = 'insertDataset NATS error, %s, trace:\n%s' % (str(exp), traceback.format_exc())
                    self.logger.warning(err)
        except cjson.DecodeError as dc:
            dbsExceptionHandler("dbsException-invalid-input2", "Wrong format/data from insert BulkBlock input",  self.logger.exception, str(dc))
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except HTTPError as he:
            raise he
        except Exception as ex:
            #illegal variable name/number
            if str(ex).find("ORA-01036") != -1:
                dbsExceptionHandler("dbsException-invalid-input2", "illegal variable name/number from input",  self.logger.exception, str(ex))
            else:
                sError = "DBSWriterModel/insertBulkBlock. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
                dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    def insertFileParents(self):
        """
        API to insert a fileParents 

        :param block_name: The child block name.
        :type block_name: string
        :param child_parent_list: a list of [cid, pid] pair to insert into file_parents table
        :type child_parenyt_list: list

        """
        try:
            body = request.body.read()
            indata = cjson.decode(body)
            indata = validateJSONInputNoCopy("file_parent", indata)
            self.dbsFile.insertFileParents(indata)
        except cjson.DecodeError as dc:
            dbsExceptionHandler("dbsException-invalid-input2", "Wrong format/data from insertFileParents input",  self.logger.exception, str(dc))
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except HTTPError as he:
            raise he
        except Exception as ex:
            #illegal variable name/number
            if str(ex).find("ORA-01036") != -1:
                dbsExceptionHandler("dbsException-invalid-input2", "illegal variable name/number from input",  self.logger.exception, str(ex))
            else:
                sError = "DBSWriterModel/insertFileParents. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
                dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    def insertBlock(self):
        """
        API to insert a block into DBS

        :param blockObj: Block object
        :type blockObj: dict
        :key open_for_writing: Open For Writing (1/0) (Optional, default 1)
        :key block_size: Block Size (Optional, default 0)
        :key file_count: File Count (Optional, default 0)
        :key block_name: Block Name (Required)
        :key origin_site_name: Origin Site Name (Required)

        """
        try:
            body = request.body.read()
            indata = cjson.decode(body)
            indata = validateJSONInputNoCopy("block", indata)
            self.dbsBlock.insertBlock(indata)
        except cjson.DecodeError as dc:
            dbsExceptionHandler("dbsException-invalid-input2", "Wrong format/data from insert Block input",  self.logger.exception, str(dc))
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except Exception as ex:
            sError = "DBSWriterModel/insertBlock. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    def insertFile(self, qInserts=False):
        """
        API to insert a list of file into DBS in DBS. Up to 10 files can be inserted in one request.

        :param qInserts: True means that inserts will be queued instead of done immediately. INSERT QUEUE Manager will perform the inserts, within few minutes.
        :type qInserts: bool
        :param filesList: List of dictionaries containing following information
        :type filesList: list of dicts
        :key logical_file_name: File to be inserted (str) (Required)
        :key is_file_valid: (optional, default = 1): (bool)
        :key block: required: /a/b/c#d (str)
        :key dataset: required: /a/b/c (str)
        :key file_type: (optional, default = EDM) one of the predefined types, (str)
        :key check_sum: (optional, default = '-1') (str)
        :key event_count: (optional, default = -1) (int)
        :key file_size: (optional, default = -1.) (float)
        :key adler32: (optional, default = '') (str)
        :key md5: (optional, default = '') (str)
        :key auto_cross_section: (optional, default = -1.) (float)
        :key file_lumi_list: (optional, default = []) [{'run_num': 123, 'lumi_section_num': 12},{}....]
        :key file_parent_list: (optional, default = []) [{'file_parent_lfn': 'mylfn'},{}....]
        :key file_assoc_list: (optional, default = []) [{'file_parent_lfn': 'mylfn'},{}....]
        :key file_output_config_list: (optional, default = []) [{'app_name':..., 'release_version':..., 'pset_hash':...., output_module_label':...},{}.....]

        """
        if qInserts in (False, 'False'): qInserts=False
        try:
            body = request.body.read()
            indata = cjson.decode(body)["files"]
            if not isinstance(indata, (list, dict)):
                dbsExceptionHandler("dbsException-invalid-input", "Invalid Input DataType", self.logger.exception, \
                                      "insertFile expects input as list or dirc")
            businput = []
            if isinstance(indata, dict):
                indata = [indata]
            indata = validateJSONInputNoCopy("files", indata)
            tot_size = 0
            tot_evts = 0
            for f in indata:
                f.update({
                     #"dataset":f["dataset"],
                     "creation_date": f.get("creation_date", dbsUtils().getTime()),
                     "create_by" : dbsUtils().getCreateBy(),
                     "last_modification_date": f.get("last_modification_date", dbsUtils().getTime()),
                     "last_modified_by": f.get("last_modified_by", dbsUtils().getCreateBy()),
                     "file_lumi_list":f.get("file_lumi_list", []),
                     "file_parent_list":f.get("file_parent_list", []),
                     "file_assoc_list":f.get("assoc_list", []),
                     "file_output_config_list":f.get("file_output_config_list", [])})
                businput.append(f)
                ecount = f.get('event_count', 0)
                if ecount and ecount != None and str(ecount) != "None":
                    tot_evts += int(ecount)
                fsize = f.get('file_size', 0)
                if fsize and fsize != None and str(fsize) != "None":
                    tot_size += float(fsize)
            self.dbsFile.insertFile(businput, qInserts)
            # send message to NATS if it is configured
            if self.nats and tot_evts and tot_size:
                try:
                    doc = {'dataset':f['dataset'], 'evts': tot_evts, 'size': tot_size}
                    self.nats.publish(doc)
                except Exception as exp:
                    err = 'insertFile NATS error, %s, trace:\n%s' % (str(exp), traceback.format_exc())
                    self.logger.warning(err)
        except cjson.DecodeError as dc:
            dbsExceptionHandler("dbsException-invalid-input2", "Wrong format/data from insert File input",  self.logger.exception, str(dc))
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except HTTPError as he:
            raise he
        except Exception as ex:
            sError = "DBSWriterModel/insertFile. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @transformInputType('logical_file_name')
    @inputChecks(logical_file_name=(basestring, list), is_file_valid=(int, basestring), lost=(int, basestring, bool ), dataset=basestring)
    def updateFile(self, logical_file_name=[], is_file_valid=1, lost=0, dataset=''):
        """
        API to update file status

        :param logical_file_name: logical_file_name to update (optional), but must have either a fln or 
        a dataset
        :type logical_file_name: str
        :param is_file_valid: valid=1, invalid=0 (Required)
        :type is_file_valid: bool
        :param lost: default lost=0 (optional)
        :type lost: bool
        :param dataset: default dataset='' (optional),but must have either a fln or a dataset
        :type dataset: basestring

        """
        if lost in [1, True, 'True', 'true', '1', 'y', 'yes']:
            lost = 1
            if is_file_valid in [1, True, 'True', 'true', '1', 'y', 'yes']:
                dbsExceptionHandler("dbsException-invalid-input2", dbsExceptionCode["dbsException-invalid-input2"], self.logger.exception,\
                                    "Lost file must set to invalid" )
        else: lost = 0
        
        for f in logical_file_name, dataset:
            if '*' in f or '%' in f:
                dbsExceptionHandler("dbsException-invalid-input2", dbsExceptionCode["dbsException-invalid-input2"], self.logger.exception, "No \
                    wildcard allow in LFN or dataset for updatefile API." )
        try:
            self.dbsFile.updateStatus(logical_file_name, is_file_valid, lost, dataset)
        except HTTPError as he:
            raise he
        except Exception as ex:
            sError = "DBSWriterModel/updateFile. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(dataset=basestring, dataset_access_type=basestring)
    def updateDataset(self, dataset="", is_dataset_valid=-1, dataset_access_type=""):
        """
        API to update dataset type

        :param dataset: Dataset to update (Required)
        :type dataset: str
        :param dataset_access_type: production, deprecated, etc (Required)
        :type dataset_access_type: str

        """
        try:
            if dataset_access_type != "":
                self.dbsDataset.updateType(dataset, dataset_access_type)
                # send message to NATS if it is configured
                if self.nats:
                    try:
                        doc = {'dataset':dataset, 'dataset_type': dataset_access_type}
                        self.nats.publish(doc)
                    except Exception as exp:
                        err = 'updateDataset NATS error, %s, trace:\n%s' % (str(exp), traceback.format_exc())
                        self.logger.warning(err)
            else:
                dbsExceptionHandler("dbsException-invalid-input", "DBSWriterModel/updateDataset. dataset_access_type is required.")
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except HTTPError as he:
            raise he
        except Exception as ex:
            sError = "DBSWriterModel\updateDataset. %s\n. Exception trace: \n %s" % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(block_name=basestring, open_for_writing=(int, basestring), origin_site_name=basestring)
    def updateBlock(self, block_name="", open_for_writing=-1, origin_site_name=""):
        """
        API to update block status

        :param block_name: block name (Required)
        :type block_name: str
        :param open_for_writing: open_for_writing=0 (close), open_for_writing=1 (open) (Required)
        :type open_for_writing: str

        """
        if not block_name:
            dbsExceptionHandler('dbsException-invalid-input', "DBSBlock/updateBlock. Invalid block_name", self.logger)
        try:
            if open_for_writing != -1:
                self.dbsBlock.updateStatus(block_name, open_for_writing)
            if origin_site_name:
                self.dbsBlock.updateSiteName(block_name, origin_site_name)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except HTTPError as he:
            raise he
        except Exception as ex:
            sError = "DBSWriterModel\updateStatus. %s\n. Exception trace: \n %s" % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'],
                                self.logger.exception, sError)

    def insertDataTier(self):
        """
        API to insert A Data Tier in DBS

        :param dataTierObj: Data Tier object
        :type dataTierObj: dict
        :key data_tier_name: Data Tier that needs to be inserted

        """
        try:
            conn = self.dbi.connection()
            tran = conn.begin()

            body = request.body.read()
            indata = cjson.decode(body)

            indata = validateJSONInputNoCopy("dataTier", indata)

            indata.update({"creation_date": indata.get("creation_date", dbsUtils().getTime()), \
                           "create_by" : dbsUtils().getCreateBy()})

            indata['data_tier_id'] = self.sequenceManagerDAO.increment(conn, "SEQ_DT", tran)
            try:
                indata['data_tier_name'] = indata['data_tier_name'].upper()
            except KeyError as ke:
                dbsExceptionHandler("dbsException-invalid-input", "DBSWriterModel/insertDataTier. \
                    data_tier_name is required.")
            self.dbsDataTierInsertDAO.execute(conn, indata, tran)
            if tran: tran.commit()
        except cjson.DecodeError as dc:
            dbsExceptionHandler("dbsException-invalid-input2", "Wrong format/data from insert DataTier input",  self.logger.exception, str(dc))
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except HTTPError as he:
            raise he
        except Exception as ex:
            if str(ex).lower().find("unique constraint") != -1 or str(ex).lower().find("duplicate") != -1:
                # already exist
                self.logger.warning("Unique constraint violation being ignored...")
                self.logger.warning("%s" % ex)
                pass
            else:
                sError = " DBSWriterModel\insertDataTier. %s\n. Exception trace: \n %s" % (ex, traceback.format_exc())
                dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)
        finally:
            if tran: tran.rollback()
            if conn: conn.close()

