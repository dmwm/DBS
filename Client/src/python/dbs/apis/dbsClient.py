from dbs.exceptions.dbsClientException import dbsClientException
from RestClient.ErrorHandling.RestClientExceptions import HTTPError
from RestClient.RestApi import RestApi
from RestClient.AuthHandling.X509Auth import X509Auth
from RestClient.ProxyPlugins.Socks5Proxy import Socks5Proxy

import os, socket
import cjson

def checkInputParameter(method,parameters,validParameters,requiredParameters=None):
    """
    Helper function to check input by using before sending to the server

    :param method: Name of the API
    :type method: str
    :param validParameters: Allow parameters for the API call
    :type validParameters: list
    :param requiredParameters: Required parameters for the API call (Default: None)
    :type requiredParameters: list

    """
    for parameter in parameters:
        if parameter not in validParameters:
            raise dbsClientException("Invalid input", "API %s does not support parameter %s. Supported parameters are %s" % (method, parameter, validParameters))

    if requiredParameters is not None:
        if requiredParameters.has_key('multiple'):
            match = False
            for requiredParameter in requiredParameters['multiple']:
                if requiredParameter!='detail' and requiredParameter in parameters:
                    match = True
                    break
            if not match:
                raise dbsClientException("Invalid input", "API %s does require one of the parameters %s" % (method, requiredParameters['multiple']))
                
        if requiredParameters.has_key('forced'):
            for requiredParameter in requiredParameters['forced']:
                if requiredParameter not in parameters:
                    raise dbsClientException("Invalid input", "API %s does require the parameter %s. Forced required parameters are %s" % (method, requiredParameter,requiredParameters['forced']))

        if requiredParameters.has_key('standalone'):
            overlap = []
            for requiredParameter in requiredParameters['standalone']:
                if requiredParameter in parameters:
                    overlap.append(requiredParameter)
            if len(overlap)!=1:
                raise dbsClientException("Invalid input", "API %s does requires only *one* of the parameters %s." % (method, requiredParameters['standalone']))
                        
class DbsApi(object):
    def __init__(self, url="", proxy=None, key=None, cert=None, debug=0):
        """
        DbsApi Constructor
        
        :param url: server URL.
        :type url: str
        :param proxy: socks5 proxy format=(socks5://username:password@host:port)
        :type proxy: str
        :param key: full path to the private key to use
        :type key: str
        :param cert: full path to the certificate to use
        :type cert: str

        .. note::
           By default the DbsApi is trying to lookup the private key and the certificate in the common locations

        """
        self.url = url
        self.proxy = proxy
        self.key = key
        self.cert = cert
                
        self.rest_api = RestApi(auth=X509Auth(ssl_cert=cert, ssl_key=key),
                                proxy=Socks5Proxy(proxy_url=self.proxy) if self.proxy else None)
        
    def __callServer(self, method="", params={}, data={}, callmethod='GET', content='application/json'):
        """
        A private method to make HTTP call to the DBS Server
        
        :param method: REST API to call, e.g. 'datasets, blocks, files, ...'.
        :type method: str
        :param params: Parameters to the API call, e.g. {'dataset':'/PrimaryDS/ProcessedDS/TIER'}.
        :type params: dict
        :param callmethod: The HTTP method used, by default it is HTTP-GET, possible values are GET, POST and PUT.
        :type callmethod: str
        :param content: The type of content the server is expected to return. DBS3 only supports application/json
        :type content: str

        """
        UserID = os.environ['USER']+'@'+socket.gethostname()
        request_headers =  {"Content-Type": content, "Accept": content, "UserID": UserID }

        method_func = getattr(self.rest_api, callmethod.lower())

        data = cjson.encode(data)
        
        try:
            self.http_response = method_func(self.url, method, params, data, request_headers)
        except HTTPError as http_error:
            self.__parseForException(http_error)
                
        if content!="application/json":
            return self.http_response.body
        
        json_ret=cjson.decode(self.http_response.body)
            
        return json_ret
    
    def __parseForException(self, http_error):
        """
        An internal method, should not be used by clients

        :param httperror: Thrown httperror by the server
        """
        data = http_error.body
        try:
            if isinstance(data,str):
                data=cjson.decode(data)
        except:
            raise http_error
            
        if isinstance(data, dict) and data.has_key('exception'):# re-raise with more details
            raise HTTPError(http_error.url, data['exception'], data['message'], http_error.header, http_error.body)
        
        raise http_error

    @property
    def requestTimingInfo(self):
        """
        Returns the time needed to process the request by the frontend server in microseconds
        and the EPOC timestamp of the request in microseconds.
        :rtype: tuple containing processing time and timestamp
        """
        try:
            return tuple(item.split('=')[1] for item in self.http_response.header.get('CMS-Server-Time').split())
        except AttributeError:
            return None, None
        
    @property
    def requestContentLength(self):
        """
        Returns the content-length of the content return by the server
        :rtype: str
        """
        try:
            return self.http_response.header.get('Content-Length')
        except AttributeError:
            return None
     
    def blockDump(self,**kwargs):
        """
        API the list all information related with the block_name
        
        :param block_name: Name of block whoes children needs to be found (Required)
        :type block_name: str
        
        """
        validParameters = ['block_name']

        requiredParameters = {'forced':validParameters}

        checkInputParameter(method="blockDump",parameters=kwargs.keys(),validParameters=validParameters,requiredParameters=requiredParameters)

        return self.__callServer("blockdump",params=kwargs)

    def help(self,**kwargs):
        """
        API to get a list of supported REST APIs. In the case a particular API is specified,
        the docstring of that API is displayed.

        :param call: RESTAPI call for which help is desired (Optional)
        :type call: str
        
        """
        validParameters = ['call']

        checkInputParameter(method="help",parameters=kwargs.keys(),validParameters=validParameters)

        return self.__callServer("help",params=kwargs)

    def insertAcquisitionEra(self, acqEraObj={}):
        """
        API to insert an Acquisition Era in DBS

        :param acqEraObj: Acquisition Era object
        :type acqEraObj: dict
        :key acquisition_era_name: Acquisition Era Name (Required)
        :key start_date: start date of the acquisition era (unixtime, int) (Optional, default current date)
        :key end_date: end data of the acquisition era (unixtime, int) (Optional)

        """
        return self.__callServer("acquisitioneras", data = acqEraObj , callmethod='POST' )

    def insertBlock(self, blockObj={}):
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
        return self.__callServer("blocks", data = blockObj , callmethod='POST' )

    def insertBulkBlock(self, blockDump={}):
        """
        API to insert a bulk block
        :param blockDump: Output of the block dump command, example can be found in https://svnweb.cern.ch/trac/CMSDMWM/browser/DBS/trunk/Client/tests/dbsclient_t/unittests/blockdump.dict
        :type blockDump: dict
        
        """
        return self.__callServer("bulkblocks", data = blockDump , callmethod='POST' )

    def insertDataset(self, datasetObj={}):
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
        return self.__callServer("datasets", data = datasetObj , callmethod='POST' )
    
    def insertDataTier(self, dataTierObj={}):
        """
        API to insert A Data Tier in DBS

        :param dataTierObj: Data Tier object
        :type dataTierObj: dict
        :key data_tier_name: Data Tier that needs to be inserted

        """
        return self.__callServer("datatiers", data = dataTierObj , callmethod='POST' )

    def insertFiles(self, filesList=[], qInserts=False):
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

        if qInserts==False: #turn off qInserts
            return self.__callServer("files", params={'qInserts':qInserts}, data = filesList , callmethod='POST' )    
        return self.__callServer("files", data = filesList , callmethod='POST' )

    def insertOutputConfig(self, outputConfigObj={}):
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
        return self.__callServer("outputconfigs", data = outputConfigObj , callmethod='POST' )

    def insertPrimaryDataset(self, primaryDSObj={}):
        """
        API to insert A primary dataset in DBS

        :param primaryDSObj: primary dataset object
        :type primaryDSObj: dict
        :key primary_ds_type: TYPE (out of valid types in DBS, MC, DATA) (Required)
        :key primary_ds_name: Name of the primary dataset (Required)

        """
        return self.__callServer("primarydatasets", data = primaryDSObj, callmethod='POST' )

    def insertProcessingEra(self, procEraObj={}):
        """
        API to insert A Processing Era in DBS

        :param procEraObj: Processing Era object
        :type procEraObj: dict
        :key processing_version: Processing Version (Required)
        :key description: Description (Optional)

        """
        return self.__callServer("processingeras", data = procEraObj , callmethod='POST' )

    def listApiDocumentation(self):
        """
        API to retrieve the autogenerated documentation page from server
        """
        return self.__callServer(content="text/html")

    def listAcquisitionEras(self, **kwargs):
        """
        API to list ALL Acquisition Eras in DBS

        :param acquisition_era_name: Acquisition era name (Optional, wild cards allowed)
        :type acquisition_era_name: str
        :returns: List of dictionaries containing following keys (description, end_date, acquisition_era_name, create_by, creation_date and start_date)
        :rtype: list of dicts
        """
        validParameters = ['acquisition_era_name']
        checkInputParameter(method="listAcquisitionEras",parameters=kwargs.keys(),validParameters=validParameters)
        return self.__callServer("acquisitioneras",params=kwargs)


    def listAcquisitionEras_ci(self, **kwargs):
        """
        API to list ALL Acquisition Eras (case insensitive) in DBS

        :param acquisition_era_name: Acquisition era name (Optional, wild cards allowed)
        :type acquisition_era_name: str
        :returns: List of dictionaries containing following keys (description, end_date, acquisition_era_name, create_by, creation_date and start_date)
        :rtype: list of dicts
        
        """
        validParameters = ['acquisition_era_name']

        checkInputParameter(method="listAcquisitionEras",parameters=kwargs.keys(),validParameters=validParameters)
        
        return self.__callServer("acquisitioneras_ci",params=kwargs)

    def listBlockChildren(self, **kwargs):
        """
        API to list block children
        
        :param block_name: name of block whoes children needs to be found (Required)
        :type block_name: str
        :returns: List of dictionaries containing following keys (block_name)
        :rtype: list of dicts

        """
        validParameters=['block_name']

        requiredParameters={'forced':validParameters}

        checkInputParameter(method="listBlockChildren",parameters=kwargs.keys(),validParameters=validParameters,requiredParameters=requiredParameters)
        
        return self.__callServer("blockchildren",params=kwargs)

    def listBlockParents(self, **kwargs):
        """
        API to list block parents

        :param block_name: name of block whoes parents needs to be found (Required)
        :type block_name: str
        :returns: List of dictionaries containing following keys (block_name)
        :rtype: list of dicts

        """
        validParameters=['block_name']

        requiredParameters={'forced':validParameters}

        checkInputParameter(method="listBlockParents",parameters=kwargs.keys(),validParameters=validParameters,requiredParameters=requiredParameters)

        return self.__callServer("blockparents",params=kwargs)
    
    def listBlocks(self, **kwargs):
        """
        API to list a block in DBS. At least one of the parameters block_name, dataset or logical_file_name are required.

        :param block_name: name of the block
        :type block_name: str
        :param dataset: dataset
        :type dataset: str
        :param logical_file_name: Logical File Name
        :type logical_file_name: str
        :param origin_site_name: Origin Site Name (Optional)
        :type origin_site_name: str
        :param run_num: Run Number (Optional)
        :type run_num: int
        :param detail: Get detailed information of a block (Optional)
        :type detail: bool
        :returns: List of dictionaries containing following keys (block_name). If option detail is used the dictionaries contain the following keys (block_id, create_by, creation_date, open_for_writing, last_modified_by, dataset, block_name, file_count, origin_site_name, last_modification_date, dataset_id and block_size)
        :rtype: list of dicts

        """
        validParameters = ['dataset','block_name','origin_site_name',
                           'logical_file_name','run_num','min_cdate',
                           'max_cdate','min_ldate','max_ldate',
                           'cdate','ldate','detail']

        requiredParameters={'multiple':validParameters}

        #set defaults
        if 'detail' not in kwargs.keys():
            kwargs['detail']=False
            
        checkInputParameter(method="listBlocks",parameters=kwargs.keys(),validParameters=validParameters, requiredParameters=requiredParameters)

        return self.__callServer("blocks",params=kwargs)


    def listBlockOrigin(self, **kwargs):
        """
        API to list blocks first generated in origin_site_name 

        :param origin_site_name: Origin Site Name (Required, No wildcards)
        :type origin_site_name: str 
        :param dataset: dataset (Optional, No wildcards)
        :type dataset: str
        :returns: List of dictionaries containg the following keys (create_by, creation_date, open_for_writing, last_modified_by, dataset, block_name, file_count, origin_site_name, last_modification_date, block_size)
        :rtype: list of dicts

        """
        validParameters = ['origin_site_name', 'dataset']

        requiredParameters={'multiple':validParameters}

        checkInputParameter(method="listBlockOrigin",parameters=kwargs.keys(),validParameters=validParameters, requiredParameters=requiredParameters)


    def listDatasets(self, **kwargs):
        """
        API to list dataset(s) in DBS

        :param dataset:  Full dataset (path) of the dataset
        :type dataset: str
        :param parent_dataset: Full dataset (path) of the dataset
        :type parent_dataset: str
        :param release_version: cmssw version
        :type release_version: str
        :param pset_hash: pset hash
        :type pset_hash: str
        :param app_name: Application name (generally it is cmsRun)
        :type app_name: str
        :param output_module_label: output_module_label
        :type output_module_label: str
        :param processing_version: Processing Version
        :type processing_version: str
        :param acquisition_era_name: Acquisition Era
        :type acquisition_era_name: str
        :param primary_ds_name: Primary Dataset Name
        :type primary_ds_name: str
        :param primary_ds_type: Primary Dataset Type (Type of data, MC/DATA)
        :type primary_ds_type: str
        :param data_tier_name: Data Tier 
        :type data_tier_name: str
        :param dataset_access_type: Dataset Access Type ( PRODUCTION, DEPRECATED etc.)
        :type dataset_access_type: str
        :param prep_id: prep_id
        :type prep_id: str
        :param create_by: Creator of the dataset
        :type create_by: str
        :param last_modified_by: Last modifier of the dataset
        :type last_modified_by: str
        :param detail: List all details of a dataset
        :type detail: bool
        :returns: List of dictionaries containing the following keys (dataset). If the detail option is used. The dictionary contain the following keys (primary_ds_name, physics_group_name, acquisition_era_name, create_by, dataset_access_type, data_tier_name, last_modified_by, creation_date, processing_version, processed_ds_name, xtcrosssection, last_modification_date, dataset_id, dataset, prep_id, primary_ds_type)
        :rtype: list of dicts

        * You can use ANY combination of these parameters in this API
        * In absence of parameters, all datasets known to the DBS instance will be returned

        """
        validParameters = ['dataset','parent_dataset','is_dataset_valid',
                           'release_version','pset_hash','app_name',
                           'output_module_label','processing_version','acquisition_era_name',
                           'run_num','physics_group_name','logical_file_name',
                           'primary_ds_name','primary_ds_type','data_tier_name',
                           'dataset_access_type', 'prep_id', 'create_by', 'last_modified_by',
                           'min_cdate','max_cdate', 'min_ldate','max_ldate','cdate','ldate',
                           'detail']

        #set defaults
        if 'detail' not in kwargs.keys():
            kwargs['detail']=False

        checkInputParameter(method="listDatasets",parameters=kwargs.keys(),validParameters=validParameters)
        
        return self.__callServer("datasets",params=kwargs)

    def listDatasetAccessTypes(self, **kwargs):
        """
        API to list ALL dataset access types

        :param dataset_access_type: If provided, list that dataset access type (Optional)
        :type dataset_access_type: str
        :returns: List of dictionary containg the following key (dataset_access_type).
        :rtype: List of dicts

        """
        validParameters = ['dataset_access_type']

        checkInputParameter(method="listDatasetAccessTypes",parameters=kwargs.keys(),validParameters=validParameters)

        return self.__callServer("datasetaccesstypes",params=kwargs)

    def listDatasetArray(self, **kwargs):
        """
        API to list datasets in DBS
        
        :param dataset: list of datasets [dataset1,dataset2,..,dataset n] (Required)
        :type dataset: list
        :param dataset_access_type: If provided list only datasets having that dataset access type (Optional)
        :type dataset_access_type: str
        :param detail: brief list or detailed list 1/0
        :type detail: bool
        :returns: List of dictionaries containing the following keys (dataset). If the detail option is used. The dictionary contain the following keys (primary_ds_name, physics_group_name, acquisition_era_name, create_by, dataset_access_type, data_tier_name, last_modified_by, creation_date, processing_version, processed_ds_name, xtcrosssection, last_modification_date, dataset_id, dataset, prep_id, primary_ds_type)
        :rtype: list of dicts

        """
        validParameters = ['dataset','dataset_access_type','detail']
        requiredParameters = {'forced':['dataset']}

        checkInputParameter(method="listDatasetArray",parameters=kwargs.keys(),validParameters=validParameters,requiredParameters=requiredParameters)

        #set defaults
        if 'detail' not in kwargs.keys():
            kwargs['detail']=False

        return self.__callServer("datasetlist",data=kwargs,callmethod='POST')

    def listDatasetChildren(self, **kwargs):
        """
        API to list A datasets children in DBS

        :param dataset: dataset (Required)
        :type dataset: str
        :returns: List of dictionaries containing the following keys (child_dataset_id, child_dataset, dataset)
        :rtype: list of dicts

        """
        validParameters = ['dataset']
        requiredParameters = {'forced':validParameters}

        checkInputParameter(method="listDatasetChildren",parameters=kwargs.keys(),validParameters=validParameters,requiredParameters=requiredParameters)

        return self.__callServer("datasetchildren",params=kwargs)
    
    def listDatasetParents(self, **kwargs):
        """
        API to list A datasets parents in DBS

        :param dataset: dataset (Required)
        :type dataset: str
        :returns: List of dictionaries containing the following keys (this_dataset, parent_dataset_id, parent_dataset)
        :rtype: list of dicts

        """
        validParameters = ['dataset']
        requiredParameters = {'forced':validParameters}

        checkInputParameter(method="listDatasetParents",parameters=kwargs.keys(),validParameters=validParameters,requiredParameters=requiredParameters)
        
        return self.__callServer("datasetparents",params=kwargs)

    def listDataTiers(self, **kwargs):
        """
        API to list data tiers  known to DBS

        :param datatier: When supplied, dbs will list details on this tier (Optional)
        :type datatier: str
        :returns: List of dictionaries containing the following keys (data_tier_id, data_tier_name, create_by, creation_date)

        """
        validParameters=['data_tier_name']

        checkInputParameter(method="listDataTiers",parameters=kwargs.keys(),validParameters=validParameters)

        return self.__callServer("datatiers",params=kwargs)

    def listDataTypes(self, **kwargs):
        """
        API to list data types known to dbs (when no parameter supplied)

        :param dataset: Returns data type (of primary dataset) of the dataset (Optional)
        :type dataset: str
        :returns: List of dictionaries containing the following keys (primary_ds_type_id, data_type)
        :rtype: list of dicts

        """
        validParameters=['datatype','dataset']

        checkInputParameter(method="listDataTypes",parameters=kwargs.keys(),validParameters=validParameters)

        return self.__callServer("datatypes",params=kwargs)
    
    def listFileChildren(self, **kwargs):
        """
        API to list file children
        
        :param logical_file_name: logical_file_name of file (Required)
        :type logical_file_name: str
        :returns: List of dictionaries containing the following keys (child_logical_file_name, logical_file_name)
        :rtype: List of dicts

        """
        validParameters = ['logical_file_name']

        requiredParameters = {'forced':validParameters}

        checkInputParameter(method="listFileChildren",parameters=kwargs.keys(),validParameters=validParameters, requiredParameters=requiredParameters)
        
        return self.__callServer("filechildren",params=kwargs)

    def listFileLumis(self, **kwargs):
        """
        API to list Lumi for files. Either logical_file_name or block_name is required. No wild card support on this API
        
        :param block_name: Name of the block
        :type block_name: str
        :param logical_file_name: logical_file_name of file
        :type logical_file_name: str
        :param run_num: List lumi sections for a given run number (Optional)
        :type run_num: int
        :returns: List of dictionaries containing the following keys (lumi_section_num, logical_file_name, run_num)
        :rtype: list of dicts

        """
        validParameters = ['logical_file_name','block_name']

        requiredParameters = {'standalone':validParameters}

        checkInputParameter(method="listFileLumis",parameters=kwargs.keys(),validParameters=validParameters, requiredParameters=requiredParameters)
                
        return self.__callServer("filelumis",params=kwargs)

    def listFileParents(self, **kwargs):
        """
        API to list file parents

        :param logical_file_name: logical_file_name of file (Required)
        :type logical_file_name: str
        :returns: List of dictionaries containing the following keys (parent_logical_file_name, logical_file_name)
        :rtype: list of dicts

        """
        validParameters = ['logical_file_name','block_id','block_name']

        requiredParameters = {'standalone':validParameters}

        checkInputParameter(method="listFileParents",parameters=kwargs.keys(),validParameters=validParameters, requiredParameters=requiredParameters)

        return self.__callServer("fileparents",params=kwargs)

    def listFiles(self, **kwargs):
        """
        API to list A file in DBS. Either non-wildcarded logical_file_name, non-wildcarded dataset or non-wildcarded block_name is required.
        The combination of a non-wildcarded dataset or block_name with an wildcarded logical_file_name is supported.

        :param logical_file_name: logical_file_name of the file
        :type logical_file_name: str
        :param dataset: dataset
        :type dataset: str
        :param block_name: block name
        :type block_name: str
        :param release_version: release version
        :type release_version: str
        :param pset_hash: parameter set hash
        :type pset_hash: str
        :param app_name: Name of the application
        :type app_name: str
        :param output_module_label: name of the used output module
        :type output_module_label: str
        :param minrun: Minimal run number. If you want to look for a run range use minrun and maxrun
        :type minrun: int
        :param maxrun: Maximal run number. If you want to look for a run range use minrun and maxrun
        :type maxrun: int
        :param origin_site_name: site where the file was created
        :type origin_site_name: str
        :param lumi_list: List of lumi sections.
        :type lumi_list: list
        :param detail: Get detailed information about a file
        :type detail: bool
        :returns: List of dictionaries containing the following keys (logical_file_name). If detail parameter is true, the dictionaries contain the following keys (check_sum, branch_hash_id, adler32, block_id, event_count, file_type, create_by, logical_file_name, creation_date, last_modified_by, dataset, block_name, file_id, file_size, last_modification_date, dataset_id, file_type_id, auto_cross_section, md5, is_file_valid)
        :rtype: list of dicts

        * Run numbers must be passed as two parameters, minrun and maxrun.
        * Use minrun,maxrun for a specific run, say for runNumber 2000 use minrun=2000, maxrun=2000
        * For lumi_list the following two json formats are supported:
            - '[a1, a2, a3,]'
            - '[[a,b], [c, d],]'
        * If lumi_list is provided, one also needs to provide both minrun and maxrun parameters (equal)

        """
        validParameters = ['dataset','block_name','logical_file_name',
                          'release_version','pset_hash','app_name',
                          'output_module_label','minrun','maxrun',
                          'origin_site_name','lumi_list','detail']

        requiredParameters = {'multiple':validParameters}

        #set defaults
        if 'detail' not in kwargs.keys():
            kwargs['detail']=False

        checkInputParameter(method="listFiles",parameters=kwargs.keys(),validParameters=validParameters, requiredParameters=requiredParameters)
        
        return self.__callServer("files",params=kwargs)
        
    def listFileSummaries(self, **kwargs):
        """
        API to list number of files, event counts and number of lumis in a given block of dataset.If the optional run_num
        parameter is used, the summaries just for this run number. Either block_name or dataset name is required. No wild-cards are allowed

        :param block_name: Block name
        :type block_name: str
        :param dataset: Dataset name
        :type dataset: str
        :param run_num: Run number (Optional)
        :type run_num: int
        :returns: List of dictionaries containing the following keys (num_files, num_lumi, num_block, num_event, file_size)
        :rtype: list of dicts

        """
        validParameters = ['block_name','dataset', 'run_num']

        requiredParameters = {'standalone':['block_name', 'dataset']}

        checkInputParameter(method="listFileSummaries",parameters=kwargs.keys(),validParameters=validParameters, requiredParameters=requiredParameters)
        
        return self.__callServer("filesummaries",params=kwargs)

    def listOutputConfigs(self, **kwargs):
        """
        API to list OutputConfigs in DBS
        
        :param dataset: Full dataset (path) of the dataset
        :type dataset: str
        :param logical_file_name: logical_file_name of the file
        :type logical_file_name: str
        :param release_version: cmssw version
        :type release_version: str
        :param pset_hash: pset hash
        :type pset_hash: str
        :param app_name: Application name (generally it is cmsRun)
        :type app_name: str
        :param output_module_label: output_module_label
        :type output_module_label: str
        :param block_id: ID of the block
        :type block_id: int
        :param global_tag: Global Tag
        :type global_tag: str
        :returns: List of dictionaries containing the following keys (app_name, output_module_label, create_by, pset_hash, creation_date, release_version, global_tag, pset_name)
        :rtype: list of dicts

        * You can use ANY combination of these parameters in this API
        * All parameters are optional, if you do not provide any parameter, All configs will be listed from DBS
        
        """
        validParameters = ['dataset','logical_file_name','release_version',
                           'pset_hash','app_name','output_module_label',
                           'block_id','global_tag']

        checkInputParameter(method="listOutputConfigs",parameters=kwargs.keys(),validParameters=validParameters)
        
        return self.__callServer("outputconfigs",params=kwargs)

    def listPhysicsGroups(self, **kwargs):
        """
        API to list ALL physics groups

        :param physics_group_name: If provided, list that specific physics group
        :type physics_group_name: str
        :returns: List of dictionaries containing the following key (physics_group_name)
        :rtype: list of dicts

        """
        validParameters = ['physics_group_name']

        checkInputParameter(method="listPhysicsGroups",parameters=kwargs.keys(),validParameters=validParameters)
        
        return self.__callServer("physicsgroups",params=kwargs)

    def listPrimaryDatasets(self, **kwargs):
        """
        API to list ALL primary datasets in DBS
        
        :param primary_ds_name: If provided, will list that primary dataset
        :type primary_ds_name: str
        :param primary_ds_type:  If provided, will list all primary dataset having that type
        :type primary_ds_name: str
        :returns: List of dictionaries containing the following keys (create_by, primary_ds_type, primary_ds_id, primary_ds_name, creation_date)
        :rtype: list of dicts

        """
        validParameters = ['primary_ds_name', 'primary_ds_type']

        checkInputParameter(method="listPrimaryDatasets",parameters=kwargs.keys(),validParameters=validParameters)
        
        return self.__callServer("primarydatasets",params=kwargs)

    def listPrimaryDSTypes(self,**kwargs):
        """
        API to list primary dataset types
        
        :param primary_ds_type: If provided, will list that primary dataset type
        :type primary_ds_type: str
        :param dataset: List the primary dataset type for that dataset
        :type dataset: str
        :returns: List of dictionaries containing the following keys (primary_ds_type_id, data_type)
        :rtype: list of dicts

        """
        validParameters = ['primary_ds_type','dataset']

        checkInputParameter(method="listPrimaryDSTypes",parameters=kwargs.keys(),validParameters=validParameters)

        return self.__callServer("primarydstypes",params=kwargs)

    def listProcessingEras(self, **kwargs):
        """
        API to list all Processing Eras in DBS

        :param processing_version: Processing Version (Optional). If provided just this processing_version will be listed
        :type processing_version: str
        :returns: List of dictionaries containing the following keys (create_by, processing_version, description, creation_date)
        :rtype: list of dicts

        """
        validParameters = ['processing_version']
        requiredParameters={'forced':validParameters}
        checkInputParameter(method="listProcessingEras",parameters=kwargs.keys(),validParameters=validParameters,
                                    requiredParameters=requiredParameters)
        return self.__callServer("processingeras",params=kwargs)

    def listReleaseVersions(self, **kwargs):
        """
        API to list all release versions in DBS
        
        :param release_version: If provided, will list only that release version
        :type release_version: str
        :param dataset: If provided, will list release version of the specified dataset
        :type dataset: str
        :param logical_file_name: logical file name of the file
        :type logical_file_name: str
        :returns: List of dictionaries containing following keys (release_version)
        :rtype: list of dicts

        """
        validParameters = ['dataset','release_version', 'logical_file_name']

        checkInputParameter(method="listReleaseVersions",parameters=kwargs.keys(),validParameters=validParameters)

        return self.__callServer("releaseversions",params=kwargs)

    def listRuns(self, **kwargs):
        """
        API to list all runs in DBS. All parameters are optional.

        :param logical_file_name: List all runs in the file
        :type logical_file_name: str
        :param block_name: List all runs in the blo
        :type block_name: str
        :param dataset: List all runs in that dataset
        :type dataset: str
        :param minrun: List all runs large than minimum run number
        :type minrun: int
        :param maxrun: List all runs lower than maximum run number
        :type maxrun: int
                
        * If you omit both min/maxrun, then all runs known to DBS will be listed
        * Use minrun=maxrun for a specific run, say for runNumber 2000 use minrun=2000, maxrun=2000

        """
        validParameters = ['minrun','maxrun','logical_file_name','block_name','dataset']

        checkInputParameter(method="listRuns",parameters=kwargs.keys(),validParameters=validParameters)
        
        return self.__callServer("runs",params=kwargs)

    def submitMigration(self, migrationObj):
        """
        Submit a migration request to the migration service

        :param migrationObj: migration request object
        :type migrationObj: dict
        :key migration_url: The source DBS url for migration (required)
        :key migraiton_input: The block or dataset names to be migrated (required)

        """
        return self.__callServer("submit", data=migrationObj, callmethod='POST') 

    def statusMigration(self, **kwargs):
        """
        Check the status of migration request

        :param migration_rqst_id: Migration Request ID
        :type migration_rqst_id: str, int, long
        :param block_name: Block name
        :type block_name: str
        :param dataset: Dataset name
        :type dataset: str
        :param user: user
        :type user: str

        """
        validParameters = ['migration_rqst_id', 'block_name', 'dataset', 'user']

        requiredParameters = {'standalone':validParameters}

        checkInputParameter(method='statusMigration', parameters=kwargs.keys(), validParameters=validParameters, requiredParameters=requiredParameters)

        return self.__callServer("status", params=kwargs)

    def removeMigration(self, migrationObj):
        """
        Remove a migration request from the queue. Only FAILED (status 3) and
        PENDING (status 0) requests can be removed. Running and succeeded
        requests cannot be removed.

        :param migrationObj: migration request object
        :type migrationObj: dict
        :key migration_rqst_id: The migration request id (required)

        """
        return self.__callServer("remove", migrationObj, callmethod='POST')

    def serverinfo(self):
        """
        * API to retrieve DBS interface and status information
        * can be used as PING

        """
        return self.__callServer("serverinfo")
  
    def updateBlockStatus(self, **kwargs):
        """
        API to update block status
        
        :param block_name: block name (Required)
        :type block_name: str
        :param open_for_writing: open_for_writing=0 (close), open_for_writing=1 (open) (Required)
        :type open_for_writing: str
        
        """
        validParameters = ['block_name','open_for_writing']

        requiredParameters = {'forced':validParameters}

        checkInputParameter(method="updateBlockStatus",parameters=kwargs.keys(),validParameters=validParameters, requiredParameters=requiredParameters)

        parts=kwargs['block_name'].split('#')
        kwargs['block_name'] = xsparts[0]+urllib.quote_plus('#')+parts[1]

        return self.__callServer("blocks", params=kwargs, callmethod='PUT')

    def updateAcqEraEndDate(self, **kwargs):
        """
        API to update the end_date of an acquisition era

        :param acquisition_era_name: acquisition_era_name to update (Required)
        :type acquisition_era_name: str
        :param end_date: end_date not zero (Required)
        :type end_date: int

        """
        validParameters = ['end_date','acquisition_era_name']

        requiredParameters = {'forced':validParameters}

        checkInputParameter(method="updateAcqEraEndDate",parameters=kwargs.keys(),validParameters=validParameters, requiredParameters=requiredParameters)

        return self.__callServer("acquisitioneras", params=kwargs, callmethod='PUT')

    def updateDatasetType(self, **kwargs):
        """
        API to update dataset type

        :param dataset: Dataset to update (Required)
        :type dataset: str
        :param dataset_access_type: production, deprecated, etc (Required)
        :type dataset_access_type: str

        """
        validParameters = ['dataset','dataset_access_type']

        requiredParameters = {'forced':validParameters}

        checkInputParameter(method="updateDatasetType",parameters=kwargs.keys(),validParameters=validParameters, requiredParameters=requiredParameters)
        
        return self.__callServer("datasets", params=kwargs, callmethod='PUT')

    def updateFileStatus(self, **kwargs):
        """
        API to update file status
        
        :param logical_file_name: logical_file_name to update (Required)
        :type logical_file_name: str
        :param is_file_valid: valid=1, invalid=0 (Required)
        :type is_file_valid: bool

        """
        validParameters = ['logical_file_name','is_file_valid']

        requiredParameters= {'forced':validParameters}

        checkInputParameter(method="updateFileStatus",parameters=kwargs.keys(),validParameters=validParameters, requiredParameters=requiredParameters)
        
        return self.__callServer("files", params=kwargs, callmethod='PUT')
    
if __name__ == "__main__":
    # DBS Service URL
    url="http://cmssrv18.fnal.gov:8585/dbs3"
    #read_proxy="http://cmst0frontier1.cern.ch:3128"
    #read_proxy="http://cmsfrontier1.fnal.gov:3128"
    read_proxy=""
    api = DbsApi(url=url, proxy=read_proxy)
    print api.serverinfo()
    #print api.listPrimaryDatasets()
    #print api.listAcquisitionEras()
    #print api.listProcessingEras()
