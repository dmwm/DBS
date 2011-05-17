import os, sys, socket
import urllib, urllib2
from httplib import HTTPConnection
from StringIO import StringIO
from exceptions import Exception
import cjson

try:
    # Python 2.6
    import json
except:
    # Prior to 2.6 requires simplejson
    import simplejson as json

def checkInputParameter(method,parameters,validParameters,requiredParameters=None):
    for parameter in parameters:
        if parameter not in validParameters:
            raise Exception("DBS Client Exception","Invalid input: API %s: API does not support parameter %s. Supported parameters are %s" % (method, parameter, validParameters))

    if requiredParameters is not None:
        if requiredParameters.has_key('multiple'):
            match = False
            for requiredParameter in requiredParameters['multiple']:
                if requiredParameter!='detail' and requiredParameter in parameters:
                    match = True
                    break
            if not match:
                raise Exception("DBS Client Exception","Invalid input: API %s: API does require one of the parameters %" % (method, requiredParameters['multiple']))
                
        if requiredParameters.has_key('forced'):
            for requiredParameter in requiredParameters['forced']:
                if requiredParameter not in parameters:
                    raise Exception("DBS Client Exception","Invalid input: API %s: API does require the parameter %s. Forced required parameters are %s" % (method, requiredParameter,requiredParameters['forced']))

        if requiredParameters.has_key('standalone'):
            overlap = []
            for requiredParameter in requiredParameters['standalone']:
                if requiredParameter in parameters:
                    overlap.append(requiredParameter)
            if len(overlap)!=1:
                raise Exception("DBS Client Exception","Invalid input: API %s: API does requires only *one* of the parameters %s." % (method, requiredParameters['single']))
                        
class DbsApi(object):
    def __init__(self, url="", proxy=""):
        """
        * DbsApi CTOR
        url: serevr URL
        proxy: http proxy; this feature is TURNED OFF at the moemnt
        """
        self.url=url
        self.proxy=proxy
        self.opener =  urllib2.build_opener()

    def __callServer(self, method="", params={}, callmethod='GET'):
        """
        * __callServer 
        * A private method to make HTTP call to the DBS Server
        * method: addition to URL, this is generall where the VERB is provided for the REST call, as '/files'
        * params: parameters to server
        * callmethod; the HTTP method used, by default it is HTTP-GET, possible values are GET, POST and PUT
        """
        UserID=os.environ['USER']+'@'+socket.gethostname()
        headers =  {"Content-type": "application/json", "Accept": "application/json", "UserID": UserID }

        res=""
        try:
            calling=self.url+method
            proxies = {}
            if self.proxy not in (None, ""):
                proxies = { 'http': self.proxy }
            
            if not callmethod in ('POST', 'PUT') :
                if params == {}:
                    req = urllib2.Request(url=calling, headers = headers)
                    data = urllib2.urlopen(req)
                else:
                    parameters = urllib.urlencode(params)
                    req = urllib2.Request(url=calling+'?'+parameters, headers = headers)
                    data = urllib2.urlopen(req)
            else:
                params = cjson.encode(params)
                req = urllib2.Request(url=calling, data=params, headers = headers)
                req.get_method = lambda: callmethod
                data = self.opener.open(req)
            res = data.read()
            
        except urllib2.HTTPError, httperror:
            #print "httperror=%s" %httperror
            #self.__parseForException(json.loads(httperror.read()))
            #self.__parseForException(str(httperror))
            #HTTPError(req.get_full_url(), code, msg, hdrs, fpa)
            raise httperror
        except urllib2.URLError, urlerror:

            raise urlerror
        except Exception, e:
            raise e

        #FIXME: We will always return JSON from DBS, even from POST, PUT, DELETE APIs, make life easy here
        try:
            json_ret=json.loads(res)
            self.__parseForException(json_ret)
            return json_ret
        except Exception, e:
            raise e
		
    def __parseForException(self, data):
        """
        An internal method, should not be used by clients
        """
        if type(data)==type("abc"):
            data=json.loads(data)
        if type(data) == type({}) and data.has_key('exception'):
            raise Exception("DBS Server raised an exception: %s" %data['message'])
        return data
    
    def blockDump(self,**kwargs):
        """
        * API the list all information related with the block_name
        * block_name: name of block whoes children needs to be found --REQUIRED
        """
        validParameters = ['block_name']

        requiredParameters = {'forced':validParameters}

        checkInputParameter(method="blockDump",parameters=kwargs.keys(),validParameters=validParameters,requiredParameters=requiredParameters)

        return self.__callServer("/blockdump",params=kwargs)

    def help(self,**kwargs):
        """
        * API to get a list of supported calls
        """
        validParameters = ['call']

        checkInputParameter(method="help",parameters=kwargs.keys(),validParameters=validParameters)

        return self.__callServer("/help",params=kwargs)

    def insertAcquisitionEra(self, acqEraObj={}):
        """
        * API to insert An Acquisition Era in DBS 
        * acqEraObj : Acquisition Era object of type {}, with key(s) :-
        * acquisition_era_name : Acquisition Era Name --REQUIRED
        """
        return self.__callServer("/acquisitioneras", params = acqEraObj , callmethod='POST' )

    def insertBlock(self, blockObj={}):
        """
        * API to insert a block into DBS 
        * blockObj : block object, with key(s) :-
        * open_for_writing : Open For Writing (1/0) (Default 1)
        * block_size : Block Size (Default 0)
        * file_count : File Count (Default 0)
        * block_name : Block Name --REQUIRED
        * origin_site_name : Origin Site Name --REQUIRED 
        """
        return self.__callServer("/blocks", params = blockObj , callmethod='POST' )

    def insertBlockBluk(self, blockDump={}):
        """
        """
        return self.__callServer("/bulkblocks", params = blockDump , callmethod='POST' )

    def insertDataset(self, datasetObj={}):
        """
        * API to list A primary dataset in DBS 
        * datasetObj : dataset object of type {}, with key(s) :-
            *  processed_ds_name : Processed Dataset Name
            * primary_ds_name : Primary Dataset Name
            * is_dataset_valid : Is Dataset Valid (1/0)
            * xtcrosssection : Xtcrosssection
            * global_tag : Global Tag
            * output_configs : Output Configs (List of)
                    o app_name : App Name
                    o release_version : Release Version
                    o pset_hash : Pset Hash
                    o output_module_label : Output Module Label 
        """
        return self.__callServer("/datasets", params = datasetObj , callmethod='POST' )
    
    def insertDataTier(self, dataTierObj={}):
        """
        * API to insert A Data Tier in DBS 
        * dataTierObj : Data Tier object of type {}, with kys :-
                data_tier_name : Data Tier that needs to be inserted
        """
        return self.__callServer("/datatiers", params = dataTierObj , callmethod='POST' )

    def insertFiles(self, filesList=[], qInserts=False):
        """
        * API to insert a list of file into DBS in DBS 
        * filesList : list of file objects
        * qInserts : (NEVER use this parameter, unless you are TOLD by DBS Team)
        """

        if qInserts==False: #turn off qInserts
            return self.__callServer("/files?qInserts=%s" % qInserts, params = filesList , callmethod='POST' )    
        return self.__callServer("/files", params = filesList , callmethod='POST' )

    def insertOutputConfig(self, outputConfigObj={}):
        """
        * API to insert An OutputConfig in DBS 
        * outputConfigObj : Output Config object of type {}, with key(s) :-
        * app_name : App Name  --REQUIRED
        * release_version : Release Version --REQUIRED
        * pset_hash : Pset Hash --REQUIRED
        * output_module_label : Output Module Label --REQUIRED
        """
        return self.__callServer("/outputconfigs", params = outputConfigObj , callmethod='POST' )

    def insertPrimaryDataset(self, primaryDSObj={}):
        """
        * API to insert A primary dataset in DBS 
        * primaryDSObj : primary dataset object of type {} , with key(s) :-
        * primary_ds_type : TYPE (out of valid types in DBS, MC, DATA) --REQUIRED
        * primary_ds_name : Name of the primary dataset --REQUIRED
        """
        return self.__callServer("/primarydatasets", params = primaryDSObj, callmethod='POST' )

    def insertProcessingEra(self, procEraObj={}):
        """
        * API to insert A Processing Era in DBS 
        * procEraObj : Processing Era object of type {}
        * processing_version : Processing Version --REQUIRED
        * description : Description --REQUIRED
        """
        return self.__callServer("/processingeras", params = procEraObj , callmethod='POST' )

    def listAcquisitionEras(self, **kwargs):
        """
        * API to list ALL Acquisition Eras in DBS 
        """
        validParameters = ['acquisition_era_name']

        checkInputParameter(method="listAcquisitionEras",parameters=kwargs.keys(),validParameters=validParameters)
        
        return self.__callServer("/acquisitioneras",params=kwargs)

    def listBlockChildren(self, **kwargs):
        """
        API to list block children
        * block_name : name of block whoes children needs to be found --REQUIRED
        """
        validParameters=['block_name']

        requiredParameters={'strict':validParameters}

        checkInputParameter(method="listBlockChildren",parameters=kwargs.keys(),validParameters=validParameters,requiredParameters=requiredParameters)
        
        return self.__callServer("/blockchildren",params=kwargs)

    def listBlockParents(self, **kwargs):
        """
        API to list block parents
        * block_name : name of block whoes parents needs to be found --REQUIRED
        """
        validParameters=['block_name']

        requiredParameters={'strict':validParameters}

        checkInputParameter(method="listBlockParents",parameters=kwargs.keys(),validParameters=validParameters,requiredParameters=requiredParameters)

        return self.__callServer("/blockparents",params=kwargs)
    
    def listBlocks(self, **kwargs):
        """
        * API to list A block in DBS 
        * block_name : name of the block
        * dataset : dataset
        * logical_file_name : Logical File Name
        * origin_site_name : Origin Site Name
        * run_num : Run Number
        """
        validParameters = ['dataset','block_name','origin_site_name',
                           'logical_file_name','run_num','min_cdate',
                           'max_cdate','min_ldate','max_ldate',
                           'cdate','ldate','detail']

        #set defaults
        if 'detail' not in kwargs.keys():
            kwargs['detail']=False
            
        checkInputParameter(method="listBlocks",parameters=kwargs.keys(),validParameters=validParameters)

        return self.__callServer("/blocks",params=kwargs)

    def listDatasets(self, **kwargs):
        """
        * API to list dataset(s) in DBS 
        * dataset : Full dataset (path) of the dataset
        * parent_dataset : Full dataset (path) of the dataset
        * release_version : cmssw version
        * pset_hash : pset hash
        * app_name : Application name (generally it is cmsRun)
        * output_module_label : output_module_label
        * processing_version : Processing Version
        * acquisition_era : Acquisition Era
        * primary_ds_name : Primary Dataset Name
        * primary_ds_type : Primary Dataset Type (Type of data, MC/DATA)
        * data_tier_name : Data Tier 
        * dataset_access_type : Dataset Access Type ( PRODUCTION, DEPRECATED etc.)
        *
        * You can use ANY combination of these parameters in this API
        * In absence of parameters, all datasets know to DBS instance will be returned
        """
        validParameters = ['dataset','parent_dataset','is_dataset_valid',
                           'release_version','pset_hash','app_name',
                           'output_module_label','processing_version','acquisition_era',
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
        * API to list ALL dataset access types
        * dataset_access_type: If provided, list THAT dataset access type
        """
        validParameters = ['dataset_access_type']

        checkInputParameter(method="listDatasetAccessTypes",parameters=kwargs.keys(),validParameters=validParameters)

        return self.__callServer("/datasetaccesstypes",params=kwargs)

    def listDatasetChildren(self, **kwargs):
        """
        * API to list A datasets children in DBS 
        * dataset : dataset --REQUIRED
        """
        validParameters = ['dataset']
        requiredParameters = {'multiple':validParameters}

        checkInputParameter(method="listDatasetChildren",parameters=kwargs.keys(),validParameters=validParameters,requiredParameters=requiredParameters)

        return self.__callServer("/datasetchildren",params=kwargs)
    
    def listDatasetParents(self, **kwargs):
        """
        * API to list A datasets parents in DBS 
        * dataset : dataset --REQUIRED
        """
        validParameters = ['dataset']
        requiredParameters = {'multiple':validParameters}

        checkInputParameter(method="listDatasetParents",parameters=kwargs.keys(),validParameters=validParameters,requiredParameters=requiredParameters)
        
        return self.__callServer("/datasetparents",params=kwargs)

    def listDataTiers(self, **kwargs):
        """
        API to list data tiers  known to DBS
        datatier : when supplied, dbs will list details on this tier
        """
        validParameters=['data_tier_name']

        checkInputParameter(method="listDataTiers",parameters=kwargs.keys(),validParameters=validParameters)

        return self.__callServer("/datatiers",params=kwargs)

    def listDataTypes(self, **kwargs):
        """
        API to list data types known to dbs (when no parameter supplied)
        dataset: If provided, will return data type (of primary dataset) of the dataset
        """
        validParameters=['datatype','dataset']

        checkInputParameter(method="listDataTypes",parameters=kwargs.keys(),validParameters=validParameters)

        return self.__callServer("/datatypes",params=kwargs)
    
    def listFileChildren(self, **kwargs):
        """
        * API to list file children
        * logical_file_name : logical_file_name of file
        """
        validParameters = ['logical_file_name']

        requiredParameters = {'strict':validParameters}

        checkInputParameter(method="listFileChildren",parameters=kwargs.keys(),validParameters=validParameters, requiredParameters=requiredParameters)
        
        return self.__callServer("/filechildren",params=kwargs)

    def listFileLumis(self, **kwargs):
        """
        * API to list Lumi for files
        * logical_file_name : logical_file_name of file
        """
        validParameters = ['logical_file_name','block_name']

        requiredParameters = {'single':validParameters}

        checkInputParameter(method="listFileLumis",parameters=kwargs.keys(),validParameters=validParameters, requiredParameters=requiredParameters)
                
        return self.__callServer("/filelumis",params=kwargs)

    def listFileParents(self, **kwargs):
        """
        * API to list file parents
        * logical_file_name : logical_file_name of file
        """
        validParameters = ['logical_file_name','block_id','block_name']

        requiredParameters = {'single':validParameters}

        checkInputParameter(method="listFileParents",parameters=kwargs.keys(),validParameters=validParameters, requiredParameters=requiredParameters)

        return self.__callServer("/fileparents",params=kwargs)

    def listFiles(self, **kwargs):
        """
        * API to list A file in DBS 
        * logical_file_name : logical_file_name of file
        * dataset : dataset
        * block : block name
        * release_version : release version
        * pset_hash
        * app_name
        * output_module_label
        * minrun/maxrun : if you want to look for a run range use these 
                          Use minrun=maxrun for a specific run, say for runNumber 2000 use minrun=2000, maxrun=2000
        * origin_site_name : site where file was created
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
        * API to list number of files, event counts and number of lumis in a given block of dataset
        """
        validParameters = ['block_name','dataset']

        requiredParameters = {'single':validParameters}

        checkInputParameter(method="listFileSummaries",parameters=kwargs.keys(),validParameters=validParameters, requiredParameters=requiredParameters)
        
        return self.__callServer("/filesummaries",params=kwargs)

    def listOutputConfigs(self, **kwargs):
        """
        * API to list OutputConfigs in DBS 
        * dataset : Full dataset (path) of the dataset
        * parent_dataset : Full dataset (path) of the dataset
        * release_version : cmssw version
        * pset_hash : pset hash
        * app_name : Application name (generally it is cmsRun)
        * output_module_label : output_module_label
        * 
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
        * API to list ALL physics groups
        * physics_group_name: If provided, list THAT specific physics group
        """
        validParameters = ['physics_group_name']

        checkInputParameter(method="listPhysicsGroups",parameters=kwargs.keys(),validParameters=validParameters)
        
        return self.__callServer("/physicsgroups",params=kwargs)

    def listPrimaryDatasets(self, **kwargs):
        """
        * API to list ALL primary datasets in DBS 
        * primary_ds_name: If provided, will list THAT primary dataset
        """
        validParameters = ['primary_ds_name']

        checkInputParameter(method="listPrimaryDatasets",parameters=kwargs.keys(),validParameters=validParameters)
        
        return self.__callServer("/primarydatasets",params=kwargs)

    def listProcessingEras(self, **kwargs):
        """
        * API to list ALL Processing Eras in DBS 
        """
        validParameters = ['processing_version']

        checkInputParameter(method="listProcessingEras",parameters=kwargs.keys(),validParameters=validParameters)

        return self.__callServer("/processingeras",params=kwargs)

    def listReleaseVersions(self, **kwargs):
        """
        * API to list all release versions in DBS
        * release_version: If provided, will list THAT release version
        * dataset: If provided, will list release version of the specified dataset
        """
        validParameters = ['dataset','release_version']

        checkInputParameter(method="listReleaseVersions",parameters=kwargs.keys(),validParameters=validParameters)

        return self.__callServer("/releaseversions",params=kwargs)

    def listRuns(self, **kwargs):
        """
        * API to list runs in DBS 
        * minrun: minimum run number	
        * maxrun: maximum run number
        * (minrun, max)	defines the run range
        *
        * If you omit both min/maxrun, then all runs known to DBS will be listed
        * Use minrun=maxrun for a specific run, say for runNumber 2000 use minrun=2000, maxrun=2000
        """

        validParameters = ['minrun','maxrun','logical_file_name','block_name','dataset']

        checkInputParameter(method="listRuns",parameters=kwargs.keys(),validParameters=validParameters)
        
        return self.__callServer("/runs",params=kwargs)

    def migrateSubmit(self, inp):
        """ Submit a migrate request to migration service"""
        return self.__callServer("/submit", params=inp, callmethod='POST') 

    def migrateStatus(self, migration_request_id="", block_name="", dataset="", user=""):
        """Check the status of migration request"""
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
        * can be used a PING
        """
        return self.__callServer("/serverinfo")
  
    def updateBlockStatus(self, block_name, open_for_writing):
        """
        API to update block status
        * block_name : block name
        * open_for_writing : open_for_writing=0 (close), open_for_writing=1 (open)
        """
        parts=block_name.split('#')
        block_name=parts[0]+urllib.quote_plus('#')+parts[1]
        return self.__callServer("/blocks?block_name=%s&open_for_writing=%s" %(block_name, open_for_writing), params={}, callmethod='PUT')

    def updateDatasetStatus(self, dataset, is_dataset_valid):
        """
        API to update dataset status
        * dataset : dataset name --REQUIRED
        * is_dataset_valid : valid=1, invalid=0 --REQUIRED
        *
        """
        return self.__callServer("/datasets?dataset=%s&is_dataset_valid=%s" %(dataset, is_dataset_valid), params={}, callmethod='PUT')    

    def updateDatasetType(self, dataset, dataset_access_type):
        """
        API to update dataset status
        * dataset : Dataset --REQUIRED
        * dataset_access_type : production, deprecated, etc --REQUIRED
        *
        """
        return self.__callServer("/datasets?dataset=%s&dataset_access_type=%s" %(dataset, dataset_access_type), params={}, callmethod='PUT')    

    def updateFileStatus(self, logical_file_name="", is_file_valid=1):
        """
        API to update file status
        * logical_file_name : logical_file_name --REQUIRED
        * is_file_valid : valid=1, invalid=0 --REQUIRED
        """
        return self.__callServer("/files?logical_file_name=%s&is_file_valid=%s" %(logical_file_name, is_file_valid), params={}, callmethod='PUT')
    
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
