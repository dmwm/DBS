import os, sys, socket
import urllib, urllib2
import urlparse
import httplib
from StringIO import StringIO
import cjson
import json
from dbs.exceptions.dbsClientException import dbsClientException
from dbs.apis.dbsHTTPSAuthHandler import HTTPSAuthHandler 

def checkInputParameter(method,parameters,validParameters,requiredParameters=None):
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
    def __init__(self, url="", proxy="", key=None, cert=None, debug=0):
        """
        DbsApi CTOR
        
        :param url: server URL.
        :type url: str
        :param proxy: http proxy. This feature is TURNED OFF at the moment
        :type proxy: str

        """
        self.url = url
        self.proxy = proxy
        self.key = key
        self.cert = cert
        spliturl = urlparse.urlparse(url)
        callType = spliturl[0]
        if callType == 'http':
            self.opener =  urllib2.build_opener()
        elif callType == 'https':
            key1, cert1 = self.__getKeyCert()
            https_handler  = HTTPSAuthHandler(key1, cert1, debug)
            self.opener = urllib2.build_opener(https_handler)
        else:
            raise ValueError, "unknown URL type: %s" % callType
        
    def __callServer(self, method="", params={}, data={}, callmethod='GET', content='application/json'):
        """
        A private method to make HTTP call to the DBS Server
        
        :param method: Addition to URL, this is generall where the VERB is provided for the REST call, as '/files'.
        :type method: str
        :param params: Parameters to server.
        :type params: dict
        :param callmethod: The HTTP method used, by default it is HTTP-GET, possible values are GET, POST and PUT.
        :type callmethod: str
        :param content: The type of content the server is expected to return. Usually it is application/json 
        :type content: str
        """
        UserID=os.environ['USER']+'@'+socket.gethostname()
        headers =  {"Content-type": content, "Accept": content, "UserID": UserID }
        res=""
        try:
            calling=self.url+method
            proxies = {}
            if self.proxy not in (None, ""):
                proxies = { 'http': self.proxy }
            
            if not callmethod in ('POST', 'PUT'):
                if params == {}:
                    req = urllib2.Request(url=calling, headers = headers)
                else:
                    parameters = urllib.urlencode(params)
                    req = urllib2.Request(url=calling+'?'+parameters, headers = headers)
            else:
                data = cjson.encode(data)
                headers['Content-length'] = str(len(data))
                if params == {}:
                    req = urllib2.Request(url=calling, data=data, headers = headers)
                elif data == {}:
                    parameters = urllib.urlencode(params)
                    req = urllib2.Request(url=calling+'?'+parameters, headers = headers)
                else:
                    parameters = urllib.urlencode(params)
                    req = urllib2.Request(url=calling+'?'+parameters, data=data, headers = headers)
                req.get_method = lambda: callmethod
                
            return_data = self.opener.open(req)
            #FIXME: We need to test if there is a front end before getting request time. YG 5/2/2012
            try:
                return_info = return_data.info()
                self.request_processing_time, self.request_time = tuple(item.split('=')[1] for item in return_info.getheader('CMS-Server-Time').split())
                self.content_length = return_info.getheader('Content-Length')
            except AttributeError, ae:
                pass
            res = return_data.read()
            return_data.close()
            
        except urllib2.HTTPError, httperror:
            self.__parseForException(httperror)
        except urllib2.URLError, urlerror:
            raise urlerror
        except Exception, e:
            raise e
        
        #FIXME: We will always return JSON from DBS, even from POST, PUT, DELETE APIs, make life easy here
        if content!="application/json":
            return res
        try:
            json_ret=json.loads(res)
        except Exception, e:
            raise e
            
        return json_ret
    
    def __parseForException(self, httperror):
        """
        An internal method, should not be used by clients

        :param httperror: Throwns httperror by the server
        
        """
        data = httperror.read()
        try:
            if type(data)==type("abc"):
                data=json.loads(data)
        except ValueError as ve:
            raise httperror
            
        if type(data) == type({}) and data.has_key('exception'):# re-raise more details with more details
            raise urllib2.HTTPError(httperror.geturl(),data['exception'],data['message'],httperror.headers,httperror.fp)
        
        raise httperror

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
     
    def blockDump(self,**kwargs):
        """
        API the list all information related with the block_name
        
        :param block_name: Name of block whoes children needs to be found --REQUIRED
        :type block_name: str
        
        """
        validParameters = ['block_name']

        requiredParameters = {'forced':validParameters}

        checkInputParameter(method="blockDump",parameters=kwargs.keys(),validParameters=validParameters,requiredParameters=requiredParameters)

        return self.__callServer("/blockdump",params=kwargs)

    def help(self,**kwargs):
        """
        API to get a list of supported calls

        :param call: RESTAPI call for which help is desired
        :type call: str
        
        """
        validParameters = ['call']

        checkInputParameter(method="help",parameters=kwargs.keys(),validParameters=validParameters)

        return self.__callServer("/help",params=kwargs)

    def insertAcquisitionEra(self, acqEraObj={}):
        """
        API to insert An Acquisition Era in DBS
        
        :param acqEraObj: Acquisition Era object
        :type acqEraObj: dict
        :key acquisition_era_name: Acquisition Era Name (Required)
                
        """
        return self.__callServer("/acquisitioneras", data = acqEraObj , callmethod='POST' )

    def insertBlock(self, blockObj={}):
        """
        API to insert a block into DBS
        
        :param blockObj: Block object
        :type blockObj: dict
        :key open_for_writing: Open For Writing (1/0) (Default 1)
        :key block_size: Block Size (Default 0)
        :key file_count: File Count (Default 0)
        :key block_name: Block Name (Required)
        :key origin_site_name: Origin Site Name (Required)
        
        """
        return self.__callServer("/blocks", data = blockObj , callmethod='POST' )

    def insertBulkBlock(self, blockDump={}):
        """
        API to insert a bulk block
        :param blockDump: Output of the block dump command
        :type blockDump: dict
        
        """
        return self.__callServer("/bulkblocks", data = blockDump , callmethod='POST' )

    def insertDataset(self, datasetObj={}):
        """
        API to list A primary dataset in DBS
        
        :param datasetObj: Dataset object
        :type datasetObj: dict
        :key processed_ds_name: Processed Dataset Name
        :key primary_ds_name: Primary Dataset Name
        :key is_dataset_valid: Is Dataset Valid (1/0)
        :key xtcrosssection: Xtcrosssection
        :key global_tag: Global Tag
        :key output_configs: Output Configs (dict)
        
        """
        return self.__callServer("/datasets", data = datasetObj , callmethod='POST' )
    
    def insertDataTier(self, dataTierObj={}):
        """
        API to insert A Data Tier in DBS
        
        :param dataTierObj: Data Tier object
        :type dataTierObj: dict
        :key data_tier_name: Data Tier that needs to be inserted
        
        """
        return self.__callServer("/datatiers", data = dataTierObj , callmethod='POST' )

    def insertFiles(self, filesList=[], qInserts=False):
        """
        API to insert a list of file into DBS in DBS
        
        :param filesList: list of file objects
        :type filesList: list
        :param qInserts: NEVER use this parameter, unless you are TOLD by DBS Team
        :type qInserts: bool
        
        """

        if qInserts==False: #turn off qInserts
            return self.__callServer("/files", params={'qInserts':qInserts}, data = filesList , callmethod='POST' )    
        return self.__callServer("/files", data = filesList , callmethod='POST' )

    def insertOutputConfig(self, outputConfigObj={}):
        """
        API to insert An OutputConfig in DBS
        
        :param outputConfigObj: Output Config object
        :type outputConfigObj: dict
        :key app_name: App Name (Required)
        :key release_version: Release Version (Required)
        :key pset_hash: Pset Hash (Required)
        :key output_module_label: Output Module Label (Required)
        
        """
        return self.__callServer("/outputconfigs", data = outputConfigObj , callmethod='POST' )

    def insertPrimaryDataset(self, primaryDSObj={}):
        """
        API to insert A primary dataset in DBS
        
        :param primaryDSObj: primary dataset object
        :type primaryDSObj: dict
        :key primary_ds_type: TYPE (out of valid types in DBS, MC, DATA) (Required)
        :key primary_ds_name: Name of the primary dataset (Required)
        
        """
        return self.__callServer("/primarydatasets", data = primaryDSObj, callmethod='POST' )

    def insertProcessingEra(self, procEraObj={}):
        """
        API to insert A Processing Era in DBS
        
        :param procEraObj: Processing Era object
        :type procEraObj: dict
        :key processing_version: Processing Version (Required)
        :key description: Description (Required)
        
        """
        return self.__callServer("/processingeras", data = procEraObj , callmethod='POST' )

    def listApiDocumentation(self):
        """
        API to retrieve the documentation page from server
        """
        return self.__callServer(content="text/html")

    def listAcquisitionEras(self, **kwargs):
        """
        API to list ALL Acquisition Eras in DBS

        :param acquisition_era_name: Acquisition era name
        :type acquisition_era_name: str
        
        """
        validParameters = ['acquisition_era_name']
        checkInputParameter(method="listAcquisitionEras",parameters=kwargs.keys(),validParameters=validParameters)
        return self.__callServer("/acquisitioneras",params=kwargs)


    def listAcquisitionEras_ci(self, **kwargs):
        """
        API to list ALL Acquisition Eras in DBS

        :param acquisition_era_name: Acquisition era name
        :type acquisition_era_name: str
        
        """
        validParameters = ['acquisition_era_name']

        checkInputParameter(method="listAcquisitionEras",parameters=kwargs.keys(),validParameters=validParameters)
        
        return self.__callServer("/acquisitioneras_ci",params=kwargs)

    def listBlockChildren(self, **kwargs):
        """
        API to list block children
        
        :param block_name: name of block whoes children needs to be found (Required)
        :type block_name: str
        
        """
        validParameters=['block_name']

        requiredParameters={'forced':validParameters}

        checkInputParameter(method="listBlockChildren",parameters=kwargs.keys(),validParameters=validParameters,requiredParameters=requiredParameters)
        
        return self.__callServer("/blockchildren",params=kwargs)

    def listBlockParents(self, **kwargs):
        """
        API to list block parents
        
        :param block_name: name of block whoes parents needs to be found (Required)
        :type block_name: str
        
        """
        validParameters=['block_name']

        requiredParameters={'forced':validParameters}

        checkInputParameter(method="listBlockParents",parameters=kwargs.keys(),validParameters=validParameters,requiredParameters=requiredParameters)

        return self.__callServer("/blockparents",params=kwargs)
    
    def listBlocks(self, **kwargs):
        """
        API to list A block in DBS
        
        :param block_name: name of the block
        :type block_name: str
        :param dataset: dataset
        :type dataset: str
        :param logical_file_name: Logical File Name
        :type logical_file_name: str
        :param origin_site_name: Origin Site Name
        :type origin_site_name: str
        :param run_num: Run Number
        :type run_num: int
        
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

        return self.__callServer("/blocks",params=kwargs)

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
        
        * You can use ANY combination of these parameters in this API
        * In absence of parameters, all datasets known to DBS instance will be returned
        
        """
        validParameters = ['dataset','parent_dataset','is_dataset_valid',
                           'release_version','pset_hash','app_name',
                           'output_module_label','processing_version','acquisition_era_name',
                           'run_num','physics_group_name','logical_file_name',
                           'primary_ds_name','primary_ds_type','data_tier_name',
                           'dataset_access_type','min_cdate','max_cdate',
                           'min_ldate','max_ldate','cdate','ldate','detail']

        #set defaults
        if 'detail' not in kwargs.keys():
            kwargs['detail']=False

        checkInputParameter(method="listDatasets",parameters=kwargs.keys(),validParameters=validParameters)
        
        return self.__callServer("/datasets",params=kwargs)

    def listDatasetAccessTypes(self, **kwargs):
        """
        API to list ALL dataset access types

        :param dataset_access_type: If provided, list THAT dataset access type
        :type dataset_access_type: str
        
        """
        validParameters = ['dataset_access_type']

        checkInputParameter(method="listDatasetAccessTypes",parameters=kwargs.keys(),validParameters=validParameters)

        return self.__callServer("/datasetaccesstypes",params=kwargs)

    def listDatasetArray(self, **kwargs):
        """
        API to list datasets in DBS
        
        :param dataset: list of datasets [dataset1,dataset2,..,dataset n] (Required)
        :type dataset: list
        :param dataset_access_type: If provided list only datasets having that dataset access type
        :type dataset_access_type: str
        :param detail: brief list or detailed list 1/0
        :type detail: bool
        
        """
        validParameters = ['dataset','dataset_access_type','detail']
        requiredParameters = {'forced':['dataset']}

        checkInputParameter(method="listDatasetArray",parameters=kwargs.keys(),validParameters=validParameters,requiredParameters=requiredParameters)

        #set defaults
        if 'detail' not in kwargs.keys():
            kwargs['detail']=False

        return self.__callServer("/datasetlist",data=kwargs,callmethod='POST')

    def listDatasetChildren(self, **kwargs):
        """
        API to list A datasets children in DBS
        
        :param dataset: dataset (Required)
        :type dataset: str
        
        """
        validParameters = ['dataset']
        requiredParameters = {'forced':validParameters}

        checkInputParameter(method="listDatasetChildren",parameters=kwargs.keys(),validParameters=validParameters,requiredParameters=requiredParameters)

        return self.__callServer("/datasetchildren",params=kwargs)
    
    def listDatasetParents(self, **kwargs):
        """
        API to list A datasets parents in DBS
        
        :param dataset: dataset (Required)
        :type dataset: str
        
        """
        validParameters = ['dataset']
        requiredParameters = {'forced':validParameters}

        checkInputParameter(method="listDatasetParents",parameters=kwargs.keys(),validParameters=validParameters,requiredParameters=requiredParameters)
        
        return self.__callServer("/datasetparents",params=kwargs)

    def listDataTiers(self, **kwargs):
        """
        API to list data tiers  known to DBS
        
        :param datatier: When supplied, dbs will list details on this tier
        :type datatier: str
        
        """
        validParameters=['data_tier_name']

        checkInputParameter(method="listDataTiers",parameters=kwargs.keys(),validParameters=validParameters)

        return self.__callServer("/datatiers",params=kwargs)

    def listDataTypes(self, **kwargs):
        """
        API to list data types known to dbs (when no parameter supplied)
        
        :param dataset: If provided, will return data type (of primary dataset) of the dataset
        :type dataset: str
        
        """
        validParameters=['datatype','dataset']

        checkInputParameter(method="listDataTypes",parameters=kwargs.keys(),validParameters=validParameters)

        return self.__callServer("/datatypes",params=kwargs)
    
    def listFileChildren(self, **kwargs):
        """
        API to list file children
        
        :param logical_file_name: logical_file_name of file
        :type logical_file_name: str
        
        """
        validParameters = ['logical_file_name']

        requiredParameters = {'forced':validParameters}

        checkInputParameter(method="listFileChildren",parameters=kwargs.keys(),validParameters=validParameters, requiredParameters=requiredParameters)
        
        return self.__callServer("/filechildren",params=kwargs)

    def listFileLumis(self, **kwargs):
        """
        API to list Lumi for files
        
        :param logical_file_name: logical_file_name of file
        :type logical_file_name: str
        
        """
        validParameters = ['logical_file_name','block_name']

        requiredParameters = {'standalone':validParameters}

        checkInputParameter(method="listFileLumis",parameters=kwargs.keys(),validParameters=validParameters, requiredParameters=requiredParameters)
                
        return self.__callServer("/filelumis",params=kwargs)

    def listFileParents(self, **kwargs):
        """
        API to list file parents
        
        :param logical_file_name: logical_file_name of file
        :type logical_file_name: str
        
        """
        validParameters = ['logical_file_name','block_id','block_name']

        requiredParameters = {'standalone':validParameters}

        checkInputParameter(method="listFileParents",parameters=kwargs.keys(),validParameters=validParameters, requiredParameters=requiredParameters)

        return self.__callServer("/fileparents",params=kwargs)

    def listFiles(self, **kwargs):
        """
        API to list A file in DBS
        
        :param logical_file_name: logical_file_name of file
        :type logical_file_name: str
        :param dataset: dataset
        :type dataset: str
        :param block: block name
        :type block: str
        :param release_version: release version
        :type release_version: str
        :param pset_hash: Parameter Set Hash
        :type pset_hash: str
        :param app_name: Name of the appication
        :type app_name: str
        :param output_module_label: name of the used output module
        :type output_module_label: str
        :param minrun,maxrun: if you want to look for a run range use these 
        :type minrun,maxrun: int
        :param origin_site_name: site where file was created
        :type origin_site_name: str

        * Use minrun=maxrun for a specific run, say for runNumber 2000 use minrun=2000, maxrun=2000
        
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
        
        return self.__callServer("/files",params=kwargs)
        
    def listFileSummaries(self, **kwargs):
        """
        API to list number of files, event counts and number of lumis in a given block of dataset

        :param block_name: Block name
        :type block_name: str
        :param dataset: Dataset name
        :type dataset: str
        
        """
        validParameters = ['block_name','dataset']

        requiredParameters = {'standalone':validParameters}

        checkInputParameter(method="listFileSummaries",parameters=kwargs.keys(),validParameters=validParameters, requiredParameters=requiredParameters)
        
        return self.__callServer("/filesummaries",params=kwargs)

    def listOutputConfigs(self, **kwargs):
        """
        API to list OutputConfigs in DBS
        
        :param dataset: Full dataset (path) of the dataset
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
        
        * You can use ANY combination of these parameters in this API
        * All parameters are optional, if you do not provide any parameter, ALL configs will be listed from DBS
        
        """
        validParameters = ['dataset','logical_file_name','release_version',
                           'pset_hash','app_name','output_module_label',
                           'block_id','global_tag']

        checkInputParameter(method="listOutputConfigs",parameters=kwargs.keys(),validParameters=validParameters)
        
        return self.__callServer("/outputconfigs",params=kwargs)

    def listPhysicsGroups(self, **kwargs):
        """
        API to list ALL physics groups
        
        :param physics_group_name: If provided, list THAT specific physics group
        :type physics_group_name: str
        
        """
        validParameters = ['physics_group_name']

        checkInputParameter(method="listPhysicsGroups",parameters=kwargs.keys(),validParameters=validParameters)
        
        return self.__callServer("/physicsgroups",params=kwargs)

    def listPrimaryDatasets(self, **kwargs):
        """
        API to list ALL primary datasets in DBS
        
        :param primary_ds_name: If provided, will list THAT primary dataset
        :type primary_ds_name: str
        
        """
        validParameters = ['primary_ds_name']

        checkInputParameter(method="listPrimaryDatasets",parameters=kwargs.keys(),validParameters=validParameters)
        
        return self.__callServer("/primarydatasets",params=kwargs)

    def listPrimaryDSTypes(self,**kwargs):
        """
        API to list primary dataset types
        
        :param primary_ds_type: If provided, will list THAT primary dataset type
        :type primary_ds_type: str
        :param dataset: List the primary dataset type for dataset
        :type dataset: str
        
        """
        validParameters = ['primary_ds_type','dataset']

        checkInputParameter(method="listPrimaryDSTypes",parameters=kwargs.keys(),validParameters=validParameters)

        return self.__callServer("/primarydstypes",params=kwargs)

    def listProcessingEras(self, **kwargs):
        """
        API to list ALL Processing Eras in DBS

        :param processing_version: Processing Version
        :type processing_version: str
        
        """
        validParameters = ['processing_version']
        requiredParameters={'forced':validParameters}
        checkInputParameter(method="listProcessingEras",parameters=kwargs.keys(),validParameters=validParameters,
                                    requiredParameters=requiredParameters)
        return self.__callServer("/processingeras",params=kwargs)

    def listReleaseVersions(self, **kwargs):
        """
        API to list all release versions in DBS
        
        :param release_version: If provided, will list THAT release version
        :type release_version: str
        :param dataset: If provided, will list release version of the specified dataset
        :type dataset: str
        
        """
        validParameters = ['dataset','release_version']

        checkInputParameter(method="listReleaseVersions",parameters=kwargs.keys(),validParameters=validParameters)

        return self.__callServer("/releaseversions",params=kwargs)

    def listRuns(self, **kwargs):
        """
        API to list runs in DBS
        
        :param minrun: minimum run number
        :type minrun: int
        :param maxrun: maximum run number
        :type masrun: int
                
        * If you omit both min/maxrun, then all runs known to DBS will be listed
        * Use minrun=maxrun for a specific run, say for runNumber 2000 use minrun=2000, maxrun=2000
        
        """

        validParameters = ['minrun','maxrun','logical_file_name','block_name','dataset']

        checkInputParameter(method="listRuns",parameters=kwargs.keys(),validParameters=validParameters)
        
        return self.__callServer("/runs",params=kwargs)

    def migrateSubmit(self, inp):
        """
        Submit a migrate request to migration service

        :param inp:
        
        """
        return self.__callServer("/submit", data=inp, callmethod='POST') 

    def migrateStatus(self, migration_request_id="", block_name="", dataset="", user=""):
        """
        Check the status of migration request

        :param migration_request_id: Migration Request ID
        :type migration_request_id: str
        :param block_name: Block name
        :type block_name: str
        :param dataset: Dataset name
        :type dataset: str
        :param user: user
        :type user: str

        """
        amp=False
        add_to_url=""
        if  migration_request_id:
            add_to_url+="migration_request_id=%s" %migration_request_id
            amp=True
        if dataset:
            if amp: add_to_url += "&"
            add_to_url+="dataset=%s" %dataset
            amp=True
        if block_name:
            parts=block_name.split('#')
            block_name=parts[0]+urllib.quote_plus('#')+parts[1]
            if amp: add_to_url += "&"
            add_to_url += "block_name=%s" %block_name
            amp=True
        if user:
            if amp: add_to_url += "&"
            add_to_url += "user=%s" %user
        if add_to_url:
            return self.__callServer("/status?%s" % add_to_url )
        return self.__callServer("/status")
		
    def serverinfo(self):
        """
        * API to retrieve DAS interface and status information
        * can be used as PING

        """
        return self.__callServer("/serverinfo")
  
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

        return self.__callServer("/blocks", params=kwargs, callmethod='PUT')

    def updateAcqEraEndDate(self, **kwargs):
        """
        API to update the end_date of an acquisition era

        :acquisition_era_name: str  (Required)
        :end_date: int and not zero (Required)        
        """
        validParameters = ['end_date','acquisition_era_name']

        requiredParameters = {'forced':validParameters}

        checkInputParameter(method="updateAcqEraEndDate",parameters=kwargs.keys(),validParameters=validParameters, requiredParameters=requiredParameters)

        return self.__callServer("/acquisitioneras", params=kwargs, callmethod='PUT')

    def updateDatasetType(self, **kwargs):
        """
        API to update dataset type

        :param dataset: Dataset (Required)
        :type dataset: str
        :param dataset_access_type: production, deprecated, etc (Required)
        :type dataset_access_type: str
        
        """
        validParameters = ['dataset','dataset_access_type']

        requiredParameters = {'forced':validParameters}

        checkInputParameter(method="updateDatasetType",parameters=kwargs.keys(),validParameters=validParameters, requiredParameters=requiredParameters)
        
        return self.__callServer("/datasets", params=kwargs, callmethod='PUT')    

    def updateFileStatus(self, **kwargs):
        """
        API to update file status
        
        :param logical_file_name: logical_file_name (Required)
        :type logical_file_name: str
        :param is_file_valid: valid=1, invalid=0 (Required)
        :type is_file_valid: bool
        
        """
        validParameters = ['logical_file_name','is_file_valid']

        requiredParameters= {'forced':validParameters}

        checkInputParameter(method="updateFileStatus",parameters=kwargs.keys(),validParameters=validParameters, requiredParameters=requiredParameters)
        
        return self.__callServer("/files", params=kwargs, callmethod='PUT')
    
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
