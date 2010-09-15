#!/usr/bin/env python
"""
DBS Rest Model module
"""

__revision__ = "$Id: DBSWriterModel.py,v 1.22 2010/02/08 22:43:30 afaq Exp $"
__version__ = "$Revision: 1.22 $"

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
        self.addService('POST', 'sites', self.insertSite)
        self.addService('POST', 'blocks', self.insertBlock)
        self.addService('POST', 'files', self.insertFile)

    def insertPrimaryDataset(self):
        """
	Inserts a Primary Dataset in DBS
        Gets the input from cherrypy request body.
        input must be a dictionary with the following two keys:
        primary_ds_name, primary_ds_type
        """


	userDN = request.headers.get('Ssl-Client-S-Dn', None)
	access = request.headers.get('Ssl-Client-Verify', None)
	if userDN != '(null)' and access == 'SUCCESS':
	    self.logger.warning("<<<<<<<<<<<<<<<<<<<<<<<<<NO USER DN specified>>>>>>>>>>>>>>>>>>>>>>>")
	    # Means that the user certificate was authenticated by the frontend
	else:
	    self.logger.warning("<<<<<<<<<<<<<<<<<<<<<<<<<USER DN %s specified>>>>>>>>>>>>>>>>>>>>>>>" %userDN)

	
	try :
        	body = request.body.read()
        	indata = cjson.decode(body)
        	indata.update({"creation_date": dbsUtils().getTime(), "create_by": dbsUtils().getCreateBy() })
        	self.dbsPrimaryDataset.insertPrimaryDataset(indata)
		
	except Exception, ex:
		raise Exception ("DBS Server Exception: %s \n. Exception trace: \n %s " % (ex, traceback.format_exc()) )
		#response.status = 400
		#response.reason="DBS Server Exception: %s \n. Exception trace: \n %s " % (ex, traceback.format_exc())
		#return {"Exception" : "DBS Server Exception: %s \n. Exception trace: \n %s " % (ex, traceback.format_exc())}
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
       		raise Exception ("DBS Server Exception: %s \n. Exception trace: \n %s " % (ex, traceback.format_exc()) ) 

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
                      "origin_site": indata.get("origin_site"),
                      "open_for_writing": indata.get("open_for_writing", 1)
                      })

	    self.logger.warning(block)
	    self.dbsBlock.insertBlock(block)
    
	except Exception, ex:
	    raise Exception ("DBS Server Exception: %s \n. Exception trace: \n %s " % (ex, traceback.format_exc()) )
	    
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
	    self.logger.warning(businput) 
	    self.dbsFile.insertFile(businput)
    
	except Exception, ex:
	    raise Exception ("DBS Server Exception: %s \n. Exception trace: \n %s " % (ex, traceback.format_exc()) )
    
