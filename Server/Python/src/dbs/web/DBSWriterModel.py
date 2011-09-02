#!/usr/bin/env python
#pylint: disable=C0103
"""
DBS Rest Model module
"""

__revision__ = "$Id: DBSWriterModel.py,v 1.46 2010/08/12 19:00:01 afaq Exp $"
__version__ = "$Revision: 1.46 $"

import re
import cjson
#from cherrypy.lib import profiler

from cherrypy import request, tools, HTTPError
from WMCore.DAOFactory import DAOFactory
from dbs.utils.dbsUtils import dbsUtils 
from dbs.web.DBSReaderModel import DBSReaderModel
from dbs.utils.dbsException import dbsException, dbsExceptionCode
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.DBSInputValidation import *

import traceback

def authInsert(user, role, group, site):
    """
    Authorization function for general insert  
    """

    # user = {
    #    'dn': '/DC=ch/DC=cern/[...]/CN=fakeuser/CN=Fake User',
    #    'method': 'X509Cert',
    #    'login': 'fakeuser',
    #    'name': 'Fake User',
    #    'roles': {
    #        'admin':   {'site': set(['t2-br-uerj', 't1-ch-cern']), 
    #                    'group': set(['cms', 'ph-users'])},
    #        'dev':     {'site': set(['t1-ch-cern']), 
    #                    'group': set(['dmwm'])},
    #        'shifter': {'site': set(['t0-ch-cern']), 
    #                    'group': set(['facop','ph-users'])}
    #    } 
    # }

    for k, v in user['roles'].iteritems():
        for g in v['group']:
            if g == 'dbs' and k == 'dbsoperator':
                return True
            elif g == 'dataops' and k == 'production-operator':
                return True
    return False

def authKeyInsert(user, role, group, site):
    """
    Authorization function for keys' insertion, such as insert into DATA_TIERS and so on.
    """
    for k, v in user['roles'].iteritems():
        for g in v['group']:
            if g == 'dbs' and k == 'dbsoperator':
                return True
    return False

class DBSWriterModel(DBSReaderModel):
    """
    DBS3 Server API Documentation 
    """
    #p=profiler.Profiler("/uscms/home/yuyi/dbs3-test/DBS/Server/Python/control")

    def __init__(self, config):

        """
        All parameters are provided through DBSConfig module
        """

        DBSReaderModel.__init__(self, config)

        self.sequenceManagerDAO = self.daofactory(classname="SequenceManager")
        self.dbsDataTierInsertDAO = self.daofactory(classname="DataTier.Insert")
        
        self._addMethod('POST', 'primarydatasets', self.insertPrimaryDataset)
        self._addMethod('POST', 'outputconfigs', self.insertOutputConfig)
        self._addMethod('POST', 'acquisitioneras', self.insertAcquisitionEra)
        self._addMethod('POST', 'processingeras', self.insertProcessingEra)
        self._addMethod('POST', 'datasets', self.insertDataset)
        self._addMethod('POST', 'blocks', self.insertBlock)
        self._addMethod('POST', 'files', self.insertFile, args=['qInserts'])
        self._addMethod('PUT', 'files', self.updateFile, 
                        args=['logical_file_name', 'is_file_valid'])
        self._addMethod('PUT', 'datasets', self.updateDataset, 
                        args=['dataset', 'is_dataset_valid',
                              'dataset_access_type'])
        self._addMethod('PUT', 'blocks', self.updateBlock,
                        args=['block_name', 'open_for_writing'])
        self._addMethod('POST', 'datatiers', self.insertDataTier)
        self._addMethod('POST', 'bulkblocks', self.insertBulkBlock)

    @tools.secmodv2(authzfunc=authInsert) 
    def insertPrimaryDataset(self):
        """
        Inserts a Primary Dataset in DBS
        Gets the input from cherrypy request body.
        input must be a dictionary with the following two keys:
        primary_ds_name, primary_ds_type
        """
        try :
            body = request.body.read()
            indata = cjson.decode(body)
            indata = validateJSONInputNoCopy("primds",indata)
            indata.update({"creation_date": dbsUtils().getTime(), "create_by": dbsUtils().getCreateBy() })
            self.dbsPrimaryDataset.insertPrimaryDataset(indata)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except HTTPError as he:
            raise he 
        except Exception, ex:
            sError = "DBSWriterModel/insertPrimaryDataset. %s\n Exception trace: \n %s" \
                        % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @tools.secmodv2(authzfunc=authInsert)
    def insertOutputConfig(self):
        """
        Insert an output configuration (formely known as algorithm config).
        Gets the input from cherrypy request body.
        input must be a dictionary with at least the following keys:
        app_name, release_version, pset_hash , output_module_label

        """

        try:
            body = request.body.read()
            indata = cjson.decode(body)
            indata = validateJSONInputNoCopy("dataset_conf_list",indata)
            indata.update({"creation_date": dbsUtils().getTime(),
                           "create_by" : dbsUtils().getCreateBy() ,
                           "last_modified_by" : dbsUtils().getCreateBy() })
            self.dbsOutputConfig.insertOutputConfig(indata)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except HTTPError as he:
            raise he
        except Exception, ex:
            sError = "DBSWriterModel/insertOutputConfig. %s\n. Exception trace: \n %s" \
                            % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @tools.secmodv2(authzfunc=authInsert)
    def insertAcquisitionEra(self):
        """
        Insert an AcquisitionEra in DBS
        Gets the input from cherrypy request body.
        input must be a dictionary with at least the following keys:
        'acquisition_era_name'

        """

        try:
            body = request.body.read()
            indata = cjson.decode(body)
            indata = validateJSONInputNoCopy("acquisition_era",indata)
            indata.update({"creation_date": indata.get("creation_date", dbsUtils().getTime()), \
                           "create_by" : indata.get("create_by", dbsUtils().getCreateBy()) })
            self.dbsAcqEra.insertAcquisitionEra(indata)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except HTTPError as he:
            raise he
        except Exception, ex:
            sError = " DBSWriterModel/insertAcquisitionEra. %s\n. Exception trace: \n %s" \
                        % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @tools.secmodv2(authzfunc=authInsert)
    def insertProcessingEra(self):
        """
        Insert an ProcessingEra in DBS
        Gets the input from cherrypy request body.
        input must be a dictionary with at least the following keys:
        'processing_version', 'description'

        """
        try:
            body = request.body.read()
            indata = cjson.decode(body)
            indata = validateJSONInputNoCopy('processing_era', indata)
            indata.update({"creation_date": indata.get("creation_date", dbsUtils().getTime()), \
                           "create_by" : indata.get("create_by", dbsUtils().getCreateBy()) })
            self.dbsProcEra.insertProcessingEra(indata)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except HTTPError as he:
            raise he
        except Exception, ex:
            sError = "DBSWriterModel/insertProcessingEra. %s\n. Exception trace: \n %s" \
                            % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)
    @tools.secmodv2(authzfunc=authInsert)                
    def insertDataset(self):
        """
        gets the input from cherrypy request body.
        input must have the following keys:
        KEYS : required/optional:default = ...
        ...
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
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except HTTPError as he:
            raise he
        except Exception, ex:
            sError = " DBSWriterModel/insertDataset. %s\n. Exception trace: \n %s" \
                        % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)
    """      
    For Profling
    @tools.secmodv2(authzfunc=authInsert)
    def insertBulkBlock(self):
        self.p.run(self._insertBulkBlock)
    """    
    @tools.secmodv2(authzfunc=authInsert)
    def insertBulkBlock(self):
        """
        gets the input from cherrypy request body.
        input must be a dictionaryi that match blockDump output.
        """
        try:
            body = request.body.read()
            indata = cjson.decode(body)
            #indata = validateJSONInput("insertBlock",indata)
            indata = validateJSONInputNoCopy("blockBulk",indata)
            self.dbsBlockInsert.putBlock(indata)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except HTTPError as he:
            raise he
        except Exception, ex:
            sError = "DBSWriterModel/insertBulkBlock. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc()) 
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)
    """
    For Profling
    
    @tools.secmodv2(authzfunc=authInsert)
    def insertBlock(self):
        self.p.run(self._insertBlock)
    """
    @tools.secmodv2(authzfunc=authInsert)
    def insertBlock(self):
        """
        gets the input from cherrypy request body.
        input must be a dictionary with the following keys:
        KEYS: required/optional : default = ...
        ...
        """	
	try:
	    body = request.body.read()
	    indata = cjson.decode(body)
            indata = validateJSONInputNoCopy("block",indata)
            vblock = re.match(r"(/[\w\d_-]+/[\w\d_-]+/[\w\d_-]+)#([\w\d_-]+)$", indata["block_name"])   
            block = {} 
            block.update({
                      "dataset" : vblock.groups()[0],
                      "creation_date" : 
                        indata.get("creation_date", dbsUtils().getTime()),
                      "create_by" : 
                        indata.get("create_by", dbsUtils().getCreateBy()),
                      "last_modification_date" : dbsUtils().getTime(),
                      "last_modified_by" : dbsUtils().getCreateBy(),
                      "block_name" : indata["block_name"],
                      "file_count" : indata.get("file_count", 0),
                      "block_size" : indata.get("block_size", 0),
                      "origin_site_name": indata.get("origin_site_name"),
                      "open_for_writing": indata.get("open_for_writing", 1),
                      })

            self.dbsBlock.insertBlock(block)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except Exception, ex:
            sError = "DBSWriterModel/insertBlock. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)
        
    @tools.secmodv2(authzfunc=authInsert)    
    def insertFile(self, qInserts=False):
        """
        gets the input from cherrypy request body
        input must be a (list of) dictionary with the following keys: <br />
        logical_file_name (required) : string  <br />
        is_file_valid: (optional, default = 1): 1/0 <br />
        block_name, required: /a/b/c#d <br />
        dataset, required: /a/b/c <br />
        file_type (optional, default = EDM): one of the predefined types, <br />
        check_sum (optional, default = '-1'): string <br />
        event_count (optional, default = -1): int <br />
        file_size (optional, default = -1.): float <br />
        adler32 (optional, default = ''): string <br />
        md5 (optional, default = ''): string <br />
        auto_cross_section (optional, default = -1.): float <br />
        file_lumi_list (optional, default = []): [{'run_num': 123, 'lumi_section_num': 12},{}....] <br />
        file_parent_list(optional, default = []) :[{'file_parent_lfn': 'mylfn'},{}....] <br />
        file_assoc_list(optional, default = []) :[{'file_parent_lfn': 'mylfn'},{}....] <br />
        file_output_config_list(optional, default = []) :[{'app_name':..., 'release_version':..., 'pset_hash':...., output_module_label':...},{}.....] <br />
        """
	if qInserts in (False, 'False'): qInserts=False
	try:
	    body = request.body.read()
	    indata = cjson.decode(body)["files"]
            if not isinstance(indata, (list,dict)):
                 dbsExceptionHandler("dbsException-invalid-input", "Invalid Input DataType", self.logger.exception, \
                                      "insertFile expects input as list or dirc")
            businput = []
            if type(indata) == dict:
                indata = [indata]
            indata = validateJSONInputNoCopy("files",indata)
            for f in indata:
                f.update({
                     #"dataset":f["dataset"],
                     "creation_date": f.get("creation_date", dbsUtils().getTime()),
                     "create_by" : f.get("create_by" , dbsUtils().getCreateBy()),
                     "last_modification_date": f.get("last_modification_date", dbsUtils().getTime()),
                     "last_modified_by": f.get("last_modified_by" , dbsUtils().getCreateBy()),
                     "file_lumi_list":f.get("file_lumi_list",[]),
                     "file_parent_list":f.get("file_parent_list",[]),
                     "file_assoc_list":f.get("assoc_list",[]),
                     "file_output_config_list":f.get("file_output_config_list",[])})
                businput.append(f)
            self.dbsFile.insertFile(businput, qInserts)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except HTTPError as he:
            raise he
        except Exception, ex:
            sError = "DBSWriterModel/insertFile. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)
 
    @inputChecks(logical_file_name=str, is_file_valid=(int, str))
    @tools.secmodv2(authzfunc=authInsert)    
    def updateFile(self, logical_file_name="", is_file_valid=1):
        """
        API to update file status
        """
        try:
            self.dbsFile.updateStatus(logical_file_name, is_file_valid)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except HTTPError as he:
            raise he
        except Exception, ex:
            sError = "DBSWriterModel/updateFile. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(dataset=str, is_dataset_valid=(int, str), dataset_access_type=(str))
    @tools.secmodv2(authzfunc=authInsert)
    def updateDataset(self, dataset="", is_dataset_valid=-1, dataset_access_type=""):
        """
        API to update dataset status
        """
        try:
            if dataset_access_type != "":
                self.dbsDataset.updateType(dataset, dataset_access_type)
            else: 
                if is_dataset_valid != -1:
                    self.dbsDataset.updateStatus(dataset, is_dataset_valid)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except HTTPError as he:
            raise he
        except Exception, ex:
            sError = "DBSWriterModel\updateDataset. %s\n. Exception trace: \n %s" % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(block_name=str, open_for_writing=(int,str))
    @tools.secmodv2(authzfunc=authInsert)
    def updateBlock(self, block_name="", open_for_writing=0):
        """
        API to update file status
        """
        try:
            self.dbsBlock.updateStatus(block_name, open_for_writing)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except HTTPError as he:
            raise he
        except Exception, ex:
            sError = "DBSWriterModel\updateStatus. %s\n. Exception trace: \n %s" % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)
    """
    For profling
    @tools.secmodv2(authzfunc=authKeyInsert)
    def insertDataTier(self):
        self.p.run(self._insertDataTier)
    """
    @tools.secmodv2(authzfunc=authKeyInsert)
    def insertDataTier(self):
	"""
	Inserts a data tier in DBS
	"""
        
	try:
            body = request.body.read()
            indata = cjson.decode(body)

            indata = validateJSONInputNoCopy("dataTier", indata)

            indata.update({"creation_date": indata.get("creation_date", dbsUtils().getTime()), \
                           "create_by" : indata.get("create_by", dbsUtils().getCreateBy()) })

            conn = self.dbi.connection()
            tran = conn.begin()

            indata['data_tier_id'] = self.sequenceManagerDAO.increment(conn, "SEQ_DT", tran)

            indata['data_tier_name'] = indata['data_tier_name'].upper()
            
            self.dbsDataTierInsertDAO.execute(conn, indata, tran)
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
            tran.commit()
            if conn:
                conn.close()

