#!/usr/bin/env python
"""
DBS Rest Model module
"""

__revision__ = "$Id: DBSWriterModel.py,v 1.46 2010/08/12 19:00:01 afaq Exp $"
__version__ = "$Revision: 1.46 $"

import re
import cjson
import threading
import time

from cherrypy import request, response, HTTPError, tools
from WMCore.WebTools.RESTModel import RESTModel
from dbs.utils.dbsExceptionDef import DBSEXCEPTIONS
from dbs.utils.dbsUtils import dbsUtils 
from dbs.web.DBSReaderModel import DBSReaderModel
#from dbs.business.DBSFileBuffer import DBSFileBuffer

import traceback

def authInsert(user,role,group,site):
    """
    Authorization function for general insert  
    """
    """
    user = {
        'dn': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=fakeuser/CN=123456/CN=Fake User',
        'method': 'X509Cert',
        'login': 'fakeuser',
        'name': 'Fake User',
        'roles': {
            'admin':   {'site': set(['t2-br-uerj', 't1-ch-cern']), 
                        'group': set(['cms', 'ph-users'])},
            'dev':     {'site': set(['t1-ch-cern']), 
                        'group': set(['dmwm'])},
            'shifter': {'site': set(['t0-ch-cern']), 
                        'group': set(['facop','ph-users'])}
        } 
    }

    """
    for k, v in user['roles'].iteritems():
        for g in v['group']:
            if g=='dbs' and k=='dbsoperator':
                return True
            elif g=='dataops' and k=='production-operator':
                return True
    return False

def authKeyInsert(user,role,group,site):
    """
    Authorization function for keys' insertion, such as insert into DATA_TIERS and so on.
    """
    for k, v in user['roles'].iteritems():
        for g in v['group']:
            if g=='dbs' and k=='dbsoperator':
                return True
    return False

class DBSWriterModel(DBSReaderModel):
    """
    DBS3 Server API Documentation 
    """
    def __init__(self, config):

        """
        All parameters are provided through DBSConfig module
        """

        DBSReaderModel.__init__(self, config)
        self._addMethod('POST', 'primarydatasets', self.insertPrimaryDataset)
        self._addMethod('POST', 'outputconfigs', self.insertOutputConfig)
	self._addMethod('POST', 'acquisitioneras', self.insertAcquisitionEra)
	self._addMethod('POST', 'processingeras', self.insertProcessingEra)
        self._addMethod('POST', 'datasets', self.insertDataset)
        self._addMethod('POST', 'sites', self.insertSite)
        self._addMethod('POST', 'blocks', self.insertBlock)
        self._addMethod('POST', 'files', self.insertFile, args=['qInserts'])
	self._addMethod('PUT', 'files', self.updateFile, args=['logical_file_name', 'is_file_valid'])
	self._addMethod('PUT', 'datasets', self.updateDataset, args=['dataset','is_dataset_valid', 'dataset_access_type'])
	self._addMethod('PUT', 'blocks', self.updateBlock, args=['block_name', 'open_for_writing'])
	self._addMethod('POST', 'datatiers', self.insertDataTier)
        self._addMethod('POST', 'bulkblocks', self.insertBulkBlock)

#self.dbsFileBuffer = DBSFileBuffer(self.logger, self.dbi, config.dbowner)
    
	#following chunk can be removed at a later point, when we are satisfied with the alternate/wmcore-component
	"""
	threading.Thread(target=self.handleBuffer).start()
    def handleBuffer(self):
	while True:
	    try :
		blks = self.dbsFileBuffer.getBlocks()
		for ablk_id in blks:
		    bufferedinput = self.dbsFileBuffer.getBufferedFiles(ablk_id["block_id"])
		    time.sleep(10)
		    insertinput = [eval(afile['file_blob'])  for afile in bufferedinput ]
		    #for afile in insertinput:
			#self.logger.debug("run_inserts : %s" % afile.keys() )
			#self.logger.debug("run_inserts : %s" % afile['file_output_config_list'] )
		    if len(insertinput) > 0:
			self.dbsFileBuffer.insertBufferedFiles(businput=insertinput)
	    except Exception, ex:
	    	raise Exception ("DBS Server Exception: %s \n. Exception trace: \n %s " % (ex, traceback.format_exc()) )
    """
    @tools.secmodv2(authzfunc=authInsert) 
    def insertPrimaryDataset(self):
        """
	Inserts a Primary Dataset in DBS
        Gets the input from cherrypy request body.
        input must be a dictionary with the following two keys:
        primary_ds_name, primary_ds_type
        """
        """
	userDN = request.headers.get('Ssl-Client-S-Dn', None)
	access = request.headers.get('Ssl-Client-Verify', None)
	if userDN != '(null)' and access == 'SUCCESS':
            self.logger.warning("DBS Web DBSWriterMOdel\n")
	    self.logger.warning("<<<<<<<<<<<<<<<<<<<<<<<<<NO USER DN specified>>>>>>>>>>>>>>>>>>>>>>>")
	    # Means that the user certificate was authenticated by the frontend
	else:
	    self.logger.warning("<<<<<<<<<<<<<<<<<<<<<<<<<USER DN %s specified>>>>>>>>>>>>>>>>>>>>>>>" %userDN)

	"""
        #print "----insertPrimaryDataset----"
	try :
        	body = request.body.read()
        	indata = cjson.decode(body)
        	indata.update({"creation_date": dbsUtils().getTime(), "create_by": dbsUtils().getCreateBy() })
        	self.dbsPrimaryDataset.insertPrimaryDataset(indata)
		
	except Exception, ex:
                #print "Throw HTTPError exception = %s" %(ex)
                if "dbsException-7" in ex.args[0]:
                    raise HTTPError(400, str(ex))
                else:
                    msg = "%s DBSWriterModel/insertPrimaryDataset. %s\n. Exception trace: \n %s" \
                        %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
                    self.logger.exception(msg) 
                    raise Exception ("dbsException-3", msg) 
    
    @tools.secmodv2(authzfunc=authInsert)
    def insertOutputConfig(self):
        """
        Insert an output configuration (formely known as algorithm config) in DBS
        Gets the input from cherrypy request body.
        input must be a dictionary with at least the following keys:
        app_name, release_version, pset_hash , output_module_label

        """

        try:
                body = request.body.read()
                indata = cjson.decode(body)
                indata.update({"creation_date": dbsUtils().getTime(), \
                                "create_by" : dbsUtils().getCreateBy() , "last_modified_by" : dbsUtils().getCreateBy() })
                # need proper validation
                self.dbsOutputConfig.insertOutputConfig(indata)

        except Exception, ex:
                if "dbsException-7" in ex.args[0]:
                    raise HTTPError(400, str(ex))
                else:
                    msg = "%s DBSWriterModel/insertOutputConfig. %s\n. Exception trace: \n %s" \
                            %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
                    self.logger.exception( msg )
                    raise Exception ("dbsException-3", msg )

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
                indata.update({"creation_date": dbsUtils().getTime(), "create_by" : dbsUtils().getCreateBy() })
                self.dbsAcqEra.insertAcquisitionEra(indata)

        except Exception, ex:
                msg = "%s DBSWriterModel/insertAcquisitionEra. %s\n. Exception trace: \n %s" \
                        %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
                self.logger.exception( msg )
                raise Exception ("dbsException-3", msg )

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
                indata.update({"creation_date": dbsUtils().getTime(), "create_by" : dbsUtils().getCreateBy() })
                self.dbsProcEra.insertProcessingEra(indata)

        except Exception, ex:
                    msg = "%s DBSWriterModel/insertProcessingEra. %s\n. Exception trace: \n %s" \
                            %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
                    self.logger.exception( msg )
                    raise Exception ("dbsException-3", msg )

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

                indata.update({"creation_date": dbsUtils().getTime(), \
				"last_modification_date" : dbsUtils().getTime(), \
				"create_by" : dbsUtils().getCreateBy() , "last_modified_by" : dbsUtils().getCreateBy() })
                
		# need proper validation
                self.dbsDataset.insertDataset(indata)

        except Exception, ex:
                if "dbsException-7" in ex.args[0]:
                    raise HTTPError(400, str(ex))
                else:
                    msg = "%s DBSWriterModel/insertDataset. %s\n. Exception trace: \n %s" \
                        %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
                    self.logger.exception(msg )
                    raise Exception ("dbsException-3", msg )
		
    def insertSite(self):
        """
	Inserts a Site in DBS
        Gets the input from cherrypy request body.
        input must be a dictionary with the following two keys:
        site_name
        """
	
	try :
        	body = request.body.read()
        	indata = cjson.decode(body)
        	self.dbsSite.insertSite(indata)
		
	except Exception, ex:
                msg = "%s DBSWriterMOdel/insertSite. %s\n. Exception trace: \n %s" \
                        %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
                self.logger.exception(msg )
                raise Exception ("dbsException-3", msg )
    
    @tools.secmodv2(authzfunc=authInsert)
    def insertBulkBlock(self):
        """
        gets the input from cherrypy request body.
        input must be a dictionaryi that match blockDump output.
        """

        try:

            body = request.body.read()
            indata = cjson.decode(body)
            #FIXME: what we should check?
            self.dbsBlockInsert.putBlock(indata)

        except Exception, ex:
            msg = "%s DBSWriterModel/insertBulkBlock. %s\n. Exception trace: \n %s" \
                    %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc()) 
            self.logger.exception(msg )
            raise Exception ("dbsException-3", msg )

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
	    # Proper validation needed
	    vblock = re.match(r"(/[\w\d_-]+/[\w\d_-]+/[\w\d_-]+)#([\w\d_-]+)$", 
                      indata["block_name"])
	    assert vblock, "Invalid block name %s" % indata["block_name"]
	    block={} 
	    block.update({
                      "dataset":vblock.groups()[0],
                      "creation_date": indata.get("creation_date", dbsUtils().getTime()),
                      "create_by" : indata.get("create_by", dbsUtils().getCreateBy()),
                      "last_modification_date" : dbsUtils().getTime(),
                      "last_modified_by" : dbsUtils().getCreateBy(),
                      "block_name":indata["block_name"],
                      "file_count":indata.get("file_count", 0),
                      "block_size":indata.get("block_size", 0),
                      "origin_site_name": indata.get("origin_site_name"),
                      "open_for_writing": indata.get("open_for_writing", 1),
                      })

	    self.dbsBlock.insertBlock(block)
    
	except Exception, ex:
            msg = "%s DBSWriterModel/insertBlock. %s\n. Exception trace: \n %s" \
                    %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
            self.logger.exception(msg )
            raise Exception ("dbsException-3", msg )
	
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
	    file_lumi_list (optional, default = []): [{"run_num": 123, "lumi_section_num": 12},{}....] <br />
	    file_parent_list(optional, default = []) :[{"file_parent_lfn": "mylfn"},{}....] <br />
	    file_assoc_list(optional, default = []) :[{"file_parent_lfn": "mylfn"},{}....] <br />
	    file_output_config_list(optional, default = []) :[{"app_name":..., "release_version":..., "pset_hash":...., output_module_label":...},{}.....] <br />
        """
	if qInserts in (False, 'False'): qInserts=False
	try:
	    body = request.body.read()
	    indata = cjson.decode(body)["files"]
        
	    # proper validation needed
	    businput = []
	    assert type(indata) in (list, dict)
	    if type(indata) == dict:
		indata = [indata]
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
    
	except Exception, ex:
            msg = "%s DBSWriterModel/insertFile. %s\n. Exception trace: \n %s" \
                    %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
            self.logger.exception(msg )
            raise Exception ("dbsException-3", msg )
  
    @tools.secmodv2(authzfunc=authInsert)    
    def updateFile(self, logical_file_name="", is_file_valid=1):
	"""
	API to update file status
	"""
	try:
	    self.dbsFile.updateStatus(logical_file_name, is_file_valid)
	except Exception, ex:
            msg = "%s DBSWriterModel/updateFile. %s\n. Exception trace: \n %s" \
                    %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
            self.logger.exception(msg )
            raise Exception ("dbsException-3", msg )

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
	except Exception, ex:
            msg = "%s DBSWriterModel\updateDataset. %s\n. Exception trace: \n %s" %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
            self.logger.exception(msg )
            raise Exception ("dbsException-3", msg )

    @tools.secmodv2(authzfunc=authInsert)
    def updateBlock(self, block_name="", open_for_writing=0):
	"""
	API to update file status
	"""
	try:
	    self.dbsBlock.updateStatus(block_name, open_for_writing)
	except Exception, ex:
            msg = "%s DBSWriterModel\updateStatus. %s\n. Exception trace: \n %s" %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
            self.logger.exception(msg )
            raise Exception ("dbsException-3", msg )

    @tools.secmodv2(authzfunc=authKeyInsert)
    def insertDataTier(self):
	"""
	Inserts a data tier in DBS
	"""

	try:
		body = request.body.read()
		indata = cjson.decode(body)
		indata.update({"creation_date": dbsUtils().getTime(), "create_by" : dbsUtils().getCreateBy() })
		self.dbsDataTier.insertDataTier(indata)
	except Exception, ex:
                msg = "%s DBSWriterModel\insertDataTier. %s\n. Exception trace: \n %s" \
                        %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
                self.logger.exception(msg )
                raise Exception ("dbsException-3", msg )
