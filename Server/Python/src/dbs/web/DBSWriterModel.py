#!/usr/bin/env python
"""
DBS Rest Model module
"""

__revision__ = "$Id: DBSWriterModel.py,v 1.13 2010/01/07 17:30:43 afaq Exp $"
__version__ = "$Revision: 1.13 $"

import re
import cjson

from cherrypy import request, response, HTTPError
from WMCore.WebTools.RESTModel import RESTModel

from dbs.utils.dbsUtils import dbsUtils
from dbs.web.DBSReaderModel import DBSReaderModel

import traceback

class DBSWriterModel(DBSReaderModel):
    """
    DBS3 Server API Documentation 
    """
    def __init__(self, config):
        """
        All parameters are provided through DBSConfig module
        """

        DBSReaderModel.__init__(self, config)

        self.addService('POST', 'primarydatasets', self.insertPrimaryDataset)
        self.addService('POST', 'outputconfigs', self.insertOutputConfig)
	self.addService('POST', 'acquisitionras', self.insertAcquisitionEra)
	self.addService('POST', 'processingeras', self.insertProcessingEra)
        self.addService('POST', 'datasets', self.insertDataset)
        self.addService('POST', 'blocks', self.insertBlock)
        self.addService('POST', 'files', self.insertFile)

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
        	indata.update({"creation_date": dbsUtils().getTime(), "create_by": dbsUtils().getCreateBy() })
        	self.dbsPrimaryDataset.insertPrimaryDataset(indata)
		
	except Exception, ex:
		response.status = 400
		#response.reason="DBS Server Exception: %s \n. Exception trace: \n %s " % (ex, traceback.format_exc())
		return {"Exception" : "DBS Server Exception: %s \n. Exception trace: \n %s " % (ex, traceback.format_exc())}
       		#raise Exception ("DBS Server Exception: %s \n. Exception trace: \n %s " % (ex, traceback.format_exc()) ) 


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
                raise Exception ("DBS Server Exception: %s \n. Exception trace: \n %s " % (ex, traceback.format_exc()) )


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
                raise Exception ("DBS Server Exception: %s \n. Exception trace: \n %s " % (ex, traceback.format_exc()) )


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
                    raise Exception ("DBS Server Exception: %s \n. Exception trace: \n %s " % (ex, traceback.format_exc()) )
			    
		
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
                raise Exception ("DBS Server Exception: %s \n. Exception trace: \n %s " % (ex, traceback.format_exc()) )

    def insertBlock(self):
        """
        gets the input from cherrypy request body.
        input must be a dictionary with the following keys:
        KEYS: required/optional : default = ...
        ...
        """

        body = request.body.read()
        indata = cjson.decode(body)

        # Proper validation needed
        vblock = re.match(r"(/[\w\d_-]+/[\w\d_-]+/[\w\d_-]+)#([\w\d_-]+)$", 
                      indata["BLOCK_NAME"])
        assert vblock, "Invalid block name %s" % indata["BLOCK_NAME"]
        block={} 
        block.update({
                      "dataset":vblock.groups()[0],
                      "creationdate": indata.get("CREATION_DATE", 123456),
                      "createby":indata.get("CREATE_BY","me"),
                      "lastmodificationdate":indata.get("LAST_MODIFICATION_DATE", 12345),
                      "lastmodifiedby":indata.get("LAST_MODIFIED_BY","me"),
                      "blockname":indata["BLOCK_NAME"],
                      "filecount":indata.get("FILE_COUNT", 0),
                      "blocksize":indata.get("BLOCK_SIZE", 0),
                      "originsite":"TEST",
                      "openforwriting":1
                      })
        self.dbsBlock.insertBlock(block)


    def insertFile(self):
        """
        gets the input from cherrypy request body
        input must be a (list of) dictionary with the following keys: <br />
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
                     "creation_date": indata.get("creation_date", dbsUtils().getTime()),
                     "create_by" : indata.get("create_by" , dbsUtils().getCreateBy()),
                     "last_modification_date": indata.get("last_modification_date", dbsUtils().getTime()),
                     "last_modified_by": indata.get("last_modified_by" , dbsUtils().getCreateBy()),
                     "file_lumi_list":f.get("file_lumi_list",[]),
                     "file_parent_list":f.get("file_parent_list",[]),
		     "file_assoc_list":f.get("assoc_list",[]),
                     "file_output_config_list":f.get("output_config_list",[])})
            businput.append(f)
            
        self.dbsFile.insertFile(businput)
    
