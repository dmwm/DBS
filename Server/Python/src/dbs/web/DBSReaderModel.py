#!/usr/bin/env python
"""
DBS Reader Rest Model module
"""

__revision__ = "$Id: DBSReaderModel.py,v 1.50 2010/08/13 20:38:37 yuyi Exp $"
__version__ = "$Revision: 1.50 $"

import cjson
import inspect
from cherrypy import request, response, HTTPError
from cherrypy import expose, tools
from WMCore.WebTools.RESTModel import RESTModel
from dbs.utils.dbsUtils import dbsUtils
from dbs.utils.dbsExceptionDef import DBSEXCEPTIONS
from dbs.business.DBSPrimaryDataset import DBSPrimaryDataset
from dbs.business.DBSDataset import DBSDataset
from dbs.business.DBSBlock import DBSBlock
from dbs.business.DBSSite import DBSSite
from dbs.business.DBSFile import DBSFile
from dbs.business.DBSAcquisitionEra import DBSAcquisitionEra
from dbs.business.DBSOutputConfig import DBSOutputConfig
from dbs.business.DBSProcessingEra import DBSProcessingEra
from dbs.business.DBSRun import DBSRun
from dbs.business.DBSDataType import DBSDataType
from dbs.business.DBSDataTier import DBSDataTier
from dbs.business.DBSStatus import DBSStatus
from dbs.business.DBSMigrate import DBSMigrate
from dbs.business.DBSBlockInsert import DBSBlockInsert
from dbs.business.DBSReleaseVersion import DBSReleaseVersion
from dbs.business.DBSDatasetAccessType import DBSDatasetAccessType
from dbs.business.DBSPhysicsGroup import DBSPhysicsGroup

import traceback
import urllib, urllib2
import re
import threading
import socket
import cjson

from cherrypy import server

__server__version__ = "$Name:  $"

class DBSReaderModel(RESTModel):
    """
    DBS3 Server API Documentation 
    """
    def __init__(self, config):
        """
        All parameters are provided through DBSConfig module
        """
        RESTModel.__init__(self, config)
        self.dbsUtils2 = dbsUtils()
        self.version = self.getServerVersion()
        #self.warning("DBSReaderModle")
        #self.logger.warning("DBSReaderModle")
	#self.register() Need to figure out details befer to trun on it. YG 1/26/11
        self.methods = {'GET':{}, 'PUT':{}, 'POST':{}, 'DELETE':{}}
	self._addMethod('GET', 'serverinfo', self.getServerInfo)
        self._addMethod('GET', 'primarydatasets', self.listPrimaryDatasets, args=['primary_ds_name', 'primary_ds_type'])
        self._addMethod('GET', 'datasets', self.listDatasets, args=['dataset', 'parent_dataset', 'release_version',\
                                'pset_hash', 'app_name', 'output_module_label', 'processing_version', \
                                'acquisition_era_name', 'run_num','physics_group_name', 'logical_file_name', \
                                'primary_ds_name', 'primary_ds_type', 'data_tier_name', 'dataset_access_type', \
                                'is_dataset_valid', 'min_cdate, max_cdate', 'min_ldate', 'max_ldate', \
                                'cdate', 'ldate', 'detail'])
        self._addMethod('GET', 'blocks', self.listBlocks, args=['dataset', 'block_name', 'origin_site_name', \
                        'logical_file_name', 'run_num', 'min_cdate', 'max_cdate', 'min_ldate', \
                        'max_ldate', 'cdate', 'ldate', 'detail'])
        self._addMethod('GET', 'files', self.listFiles, args=['dataset', 'block_name', 'logical_file_name',\
                        'release_version', 'pset_hash', 'app_name', 'output_module_label', 'minrun', 'maxrun',\
                        'origin_site_name', 'lumi_list', 'detail'])
        self._addMethod('GET', 'filesummaries', self.listFileSummaries, args=['block_name','dataset'])
        self._addMethod('GET', 'datasetparents', self.listDatasetParents, args=['dataset'])
        self._addMethod('GET', 'datasetchildren', self.listDatasetChildren, args=['dataset'])
        self._addMethod('GET', 'outputconfigs', self.listOutputConfigs, args=['dataset', 'logical_file_name',\
                        'release_version', 'pset_hash', 'app_name', 'output_module_label', 'block_id', 'global_tag'])
        self._addMethod('GET', 'fileparents', self.listFileParents, args=['logical_file_name', 'block_id', \
                        'block_name'])
        self._addMethod('GET', 'filechildren', self.listFileChildren, args=['logical_file_name'])
        self._addMethod('GET', 'filelumis', self.listFileLumis, args=['logical_file_name', 'block_name', 'run_num'])
        self._addMethod('GET', 'runs', self.listRuns, args=['minrun', 'maxrun', 'logical_file_name', \
                        'block_name', 'dataset'])
        #self._addMethod('GET', 'sites', self.listSites)
        self._addMethod('GET', 'datatypes', self.listDataTypes, args=['datatype', 'dataset'])
        self._addMethod('GET', 'datatiers', self.listDataTiers, args=['data_tier_name'])
        self._addMethod('GET', 'blockparents', self.listBlockParents, args=['block_name'])
        self._addMethod('GET', 'blockchildren', self.listBlockChildren, args=['block_name'])
        self._addMethod('GET', 'blockdump', self.dumpBlock, args=['block_name'])
        self._addMethod('GET', 'acquisitioneras', self.listAcquisitionEras, args=['acquisition_era_name'])
        self._addMethod('GET', 'processingeras', self.listProcessingEras, args=['processing_version'])
        self._addMethod('GET', 'releaseversions', self.listReleaseVersions, args=['release_version', 'dataset'])
        self._addMethod('GET', 'datasetaccesstypes', self.listDatasetAccessTypes, args=['dataset_access_type'])
        self._addMethod('GET', 'physicsgroups', self.listPhysicsGroups, args=['physics_group_name'])
	self._addMethod('GET', 'help', self.getHelp, args=['call'])
	self._addMethod('GET', 'register', self.register, args=[])

        self.dbsPrimaryDataset = DBSPrimaryDataset(self.logger, self.dbi, config.dbowner)
        self.dbsDataset = DBSDataset(self.logger, self.dbi, config.dbowner)
        self.dbsBlock = DBSBlock(self.logger, self.dbi, config.dbowner)
        self.dbsFile = DBSFile(self.logger, self.dbi, config.dbowner)
        self.dbsAcqEra = DBSAcquisitionEra(self.logger, self.dbi, config.dbowner)
        self.dbsOutputConfig = DBSOutputConfig(self.logger, self.dbi, config.dbowner)
        self.dbsProcEra = DBSProcessingEra(self.logger, self.dbi, config.dbowner)
        self.dbsSite = DBSSite(self.logger, self.dbi, config.dbowner)
	self.dbsRun = DBSRun(self.logger, self.dbi, config.dbowner)
	self.dbsDataType = DBSDataType(self.logger, self.dbi, config.dbowner)
	self.dbsDataTier = DBSDataTier(self.logger, self.dbi, config.dbowner)
	self.dbsStatus = DBSStatus(self.logger, self.dbi, config.dbowner)
	self.dbsMigrate = DBSMigrate(self.logger, self.dbi, config.dbowner)
        self.dbsBlockInsert = DBSBlockInsert(self.logger, self.dbi, config.dbowner) 
        self.dbsReleaseVersion = DBSReleaseVersion(self.logger, self.dbi, config.dbowner)
        self.dbsDatasetAccessType = DBSDatasetAccessType(self.logger, self.dbi, config.dbowner)
        self.dbsPhysicsGroup =DBSPhysicsGroup(self.logger, self.dbi, config.dbowner)
    
    def geoLocateThisHost(self, ip):
	"""
	Locate the host, otherwise return 'UNKNOWN'
	"""
	response = urllib.urlopen('http://api.hostip.info/get_html.php?ip=%s' % ip).read()
	m = re.search('City: (.*)', response)
	if m:
	    return m.group(1)
	return "UNKNOWN"
    
    def register(self):
	"""
	Method that attempts to register this service with Service Registry.
	NO Error is thrown, if the registry is inaccessible for any reason
	"""
	try:
	    srvcregistry="http://cmssrv18.fnal.gov:8686/SRVCREGISTRY/services"
	    addthis={}
	    addthis['NAME'] = self.config._internal_name
	    addthis['TYPE'] = self.__class__.__name__
	    addthis['LOCATION'] = self.geoLocateThisHost(socket.gethostbyname(socket.gethostname()))
	    addthis['STATUS'] = "WORKING"
	    #addthis['ADMIN'] = self.config.admin
	    addthis['URI'] = "%s/%s" % (server.base(), self.config._internal_name)
            #addthis['DB'] = self.config.CoreDatabase.connectUrl  #<<<<<<<<<<<remove password
	    #addthis['DB'] = self.config.database.connectUrl  #<<<<<<<<<<<remove password
	    addthis['VERSION'] = self.getServerVersion()
	    addthis['LAST_CONTACT'] = self.dbsUtils2.getTime()
	    addthis['COMMENTS'] = "DBS Service"
	    self.logger.warning("DBS Web DBSReaderModel/register. REGISTERING DBS: %s\n" \
                    %str(addthis) )
	    params = cjson.encode(addthis)
	    headers =  {"Content-type": "application/json", "Accept": "application/json" }
	    self.opener =  urllib2.build_opener()
	    req = urllib2.Request(url=srvcregistry, data=params, headers = headers)
	    req.get_method = lambda: 'POST'
	    data = self.opener.open(req)
	except Exception, ex:
	    self.logger.exception("%s DBSReaderModel/register. %s\n EXception Trace: \n %s.\n" \
                    %(DBSEXCEPTIONS['dbsException-3'],ex, traceback.format_exc()) )
	    pass
    
    def getServerVersion(self):
        """
        Reading from __version__ tag, determines the version of the DBS Server
        """
        version = __server__version__.replace("$Name: ", "")
        version = version.replace("$", "")
        version = version.strip()
        return version

    def getHelp(self, call=""):
	if call:
	    params=inspect.getargspec(self.methods['GET'][call]['call'])[0]
	    del params[params.index('self')]
	    doc = self.methods['GET'][call]['call'].__doc__
	    return dict(params=params, doc=doc)
	else:
	    return self.methods['GET'].keys()
    
    def getServerInfo(self):
        """
        Method that provides information about DBS Server to the clients
        The information includes
        * Server Version - CVS Tag
        * Schema Version - Version of Schema this DBS instance is working with
        * ETC - TBD
        """
        ret = {}
        ret["tagged_version"] = self.getServerVersion()
        ret["schema"] = self.dbsStatus.getSchemaStatus()
	ret["components"] = self.dbsStatus.getComponentStatus()
        return ret
    
    @tools.secmodv2()
    def listPrimaryDatasets(self, primary_ds_name="", primary_ds_type=""):
        """
        Example url's: <br />
        http://dbs3/primarydatasets <br />
        http://dbs3/primarydatasets/qcd_20_30 <br />
        http://dbs3/primarydatasets?primary_ds_name=qcd* <br />
	http://dbs3/primarydatasets?primary_ds_type=qcd* <br />
        """
        primary_ds_name = primary_ds_name.replace("*","%")
	primary_ds_type = primary_ds_type.replace("*","%")
        try:
            #print"-----ListPrimaryDatasets___"
            return self.dbsPrimaryDataset.listPrimaryDatasets(primary_ds_name, primary_ds_type)
        except Exception, ex:
            if "dbsException-7" in ex.args[0]:
                raise HTTPError(400, str(ex) )
            else:
                msg="%s DBSReader/listPrimaryDatasets. %s.\n Exception trace: \n %s."\
                    %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc() )
                raise Exception ("dbsException-3", msg)

    #@expose
    @tools.secmodv2()
    def listDatasets(self, dataset="", parent_dataset="", is_dataset_valid=1, release_version="", pset_hash="",\
        app_name="", output_module_label="", processing_version="", acquisition_era_name="",\
        run_num="0", physics_group_name="", logical_file_name="", primary_ds_name="",\
        primary_ds_type="", data_tier_name="", dataset_access_type="RO",\
        min_cdate='0', max_cdate='0', min_ldate='0', max_ldate='0', cdate='0', ldate='0', detail=False):
        #import pdb
        #pdb.set_trace()
        """
	This API lists the dataset paths and associated information.
	If no parameter is given, all datasets will be returned.
	<dataset> parameter can include one or several '*' as wildcards.
	<detail> parameter is defaulted to False, which means only dataset paths will be returned in the output dictionary. 
	In order to get more information, one needs to provide detail=True.
	<run_num> can be only be passed as a single number. No interval of run numbers is supported for this api for now.
        """
        dataset = dataset.replace("*", "%")
	parent_dataset = parent_dataset.replace("*", "%")
	release_version = release_version.replace("*", "%")
	pset_hash = pset_hash.replace("*", "%")
	app_name = app_name.replace("*", "%")
	output_module_label = output_module_label.replace("*", "%")
	logical_file_name = logical_file_name.replace("*", "%")
	physics_group_name = physics_group_name.replace("*", "%")
	primary_ds_name = primary_ds_name.replace("*", "%")
	primary_ds_type = primary_ds_type.replace("*", "%")
	data_tier_name = data_tier_name.replace("*", "%")
	dataset_access_type = dataset_access_type.replace("*", "%")
        acquisition_era_name = acquisition_era_name.replace("*", "%")
        processing_version =  processing_version.replace("*", "%")
        try:
            #run_num = run_num.replace("*", "%")
            if '*' in run_num or '%' in run_num:
                run_num = 0
            else:
                run_num = int(run_num)
            if '*' in min_cdate or '%' in min_cdate:
                min_cdate = 0
            else:
                min_cdate = int(min_cdate)
            if '*' in max_cdate or '%' in max_cdate:
                max_cdate = 0
            else:
                max_cdate = int(max_cdate)
            if '*' in min_ldate or '%' in min_ldate:
                min_ldate = 0
            else:
                min_ldate = int(min_ldate)
            if '*' in max_ldate or '%' in max_ldate:
                max_ldate = 0
            else:
                max_ldate = int(max_ldate)
            if '*' in cdate or '%' in cdate:
                cdate = 0
            else:
                cdate = int(cdate)
            if '*' in ldate or '%' in ldate:
                ldate = 0
            else:
                ldate = int(ldate)
        except Exception, e:
            self.debug(str(e))
            self.debug(traceback.format_exc())
            raise HTTPError(400, str(e))
    
	detail = detail in (True, 1, "True", "1", 'true')
        try:
            return self.dbsDataset.listDatasets(dataset, parent_dataset, is_dataset_valid, release_version, pset_hash, \
                app_name, output_module_label, processing_version, acquisition_era_name,\
                run_num, physics_group_name, logical_file_name, primary_ds_name, primary_ds_type, \
                data_tier_name, dataset_access_type, \
                min_cdate, max_cdate, min_ldate, max_ldate, cdate, ldate, detail)
        except Exception, ex:
            if "dbsException-7" in ex.args[0]:
                raise HTTPError(400, str(ex))
            else:
                msg = "%s DBSReaderModel/listDatasets. %s\n. Exception trace: \n %s" \
                    %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
                self.logger.exception( msg )
                raise Exception ("dbsException-3", msg )
 
    @tools.secmodv2()
    def listDataTiers(self, data_tier_name=""):
	"""
	Example url's:
	    http://dbs3/datatiers
	    http://dbs3/datatiers?data_tier_name=...
	"""
	data_tier_name = data_tier_name.replace("*","%")
        try:
            return self.dbsDataTier.listDataTiers(data_tier_name)
        except Exception, ex:
            if "dbsException-7" in ex.args[0]:
                raise HTTPError(400, str(ex))
            else:
                msg = "%s DBSReaderModel/listDataTiers. %s\n. Exception trace: \n %s" \
                    %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
                self.logger.exception( msg )
                raise Exception ("dbsException-3", msg )

    @tools.secmodv2()
    def listBlocks(self, dataset="", block_name="", origin_site_name="", logical_file_name="",run_num='-1',\
                   min_cdate='0', max_cdate='0', min_ldate='0', max_ldate='0', cdate='0',  ldate='0', detail=False):
        """
        Example url's:
        http://dbs3/blocks?dataset=myDataset ||?origin_site_name=mySite <br />
        http://dbs3/blocks?block_name=myBlock ||?origin_site_name=mySite <br />
	http://dbs3/blocks?logical_file_name=my_lfn ||?origin_site_name=mySite<br />
	http://dbs3/blocks?logical_file_name=my_lfn*?dataset=myDataset*?block_name=myBlock ||?origin_site_name=mySite<br />
        """
	#site_name is ORIGIN_SITE_NAME. We need to change the name and add REAL site_name
        dataset = dataset.replace("*","%")
        block_name = block_name.replace("*","%")
	logical_file_name = logical_file_name.replace("*","%")
	origin_site_name = origin_site_name.replace("*","%")
        run_num = str(run_num)
        try:
            #run_num = run_num.replace("*", "%")
            if '%' in run_num or '*' in run_num:
                run_num = 0
            else:
                run_num = int(run_num)
            if '*' in min_cdate or '%' in min_cdate:
                min_cdate = 0
            else:
                min_cdate = int(min_cdate)
            if '*' in max_cdate or '%' in max_cdate:
                max_cdate = 0
            else:
                max_cdate = int(max_cdate)
            if '*' in min_ldate or '%' in min_ldate:
                min_ldate = 0
            else:
                min_ldate = int(min_ldate)
            if '*' in max_ldate or '%' in max_ldate:
                max_ldate = 0
            else:
                max_ldate = int(max_ldate)
            if '*' in cdate or '%' in cdate:
                cdate = 0
            else:
                cdate = int(cdate)
            if '*' in ldate or '%' in ldate:
                ldate = 0
            else:
                ldate = int(ldate)
        except Exception, e:
            self.debug(str(e))
            self.debug(traceback.format_exc())
            raise HTTPError(400, str(e)) 
	detail = detail in (True, 1, "True", "1", 'true')
        try:
            return self.dbsBlock.listBlocks(dataset, block_name, origin_site_name, logical_file_name,run_num, \
                min_cdate, max_cdate, min_ldate, max_ldate, cdate, ldate, detail)
        except Exception, ex:
            if "dbsException-7" in ex.args[0]:
                raise HTTPError(400, str(ex))
            else:
                msg = "%s DBSReaderModel/listBlocks. %s\n. Exception trace: \n %s" \
                    %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
                self.logger.exception( msg )
                raise Exception ("dbsException-3", msg )

    @tools.secmodv2()
    def listBlockParents(self, block_name=""):
        """
        Example url's:
        http://dbs3/blockparents?block_name=/a/b/c%23*d <br />
        """
        block_name = block_name.replace("*","%")
        try:
            return self.dbsBlock.listBlockParents(block_name)
        except Exception, ex:
            if "dbsException-7" in ex.args[0]:
                raise HTTPError(400, str(ex))
            else:
                msg = "%s DBSReaderModel/listBlockParents. %s\n. Exception trace: \n %s" \
                    %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
                self.logger.exception( msg )
                raise Exception ("dbsException-3", msg )
    
    @tools.secmodv2()
    def listBlockChildren(self, block_name=""):
        """
        Example url's:
        http://dbs3/blockchildren?block_name=/a/b/c%23*d <br />
        """
        block_name = block_name.replace("*","%")
        try:
            return self.dbsBlock.listBlockChildren(block_name)
        except Exception, ex:
            if "dbsException-7" in ex.args[0]:
                raise HTTPError(400, str(ex))
            else:
                msg = "%s DBSReaderModel/listBlockChildren. %s\n. Exception trace: \n %s" \
                    %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
                self.logger.exception( msg )
                raise Exception ("dbsException-3", msg )

    @tools.secmodv2()
    def listFiles(self, dataset = "", block_name = "", logical_file_name = "", release_version="", 
	pset_hash="", app_name="", output_module_label="", minrun=-1, maxrun=-1,
	origin_site_name="", lumi_list="", detail=False):
        """
	This API returns logical file names and associated information.
	One of the following three parameters must be provided: dataset, block, logical_file_name.
	<detail> parameter is defaulted to False, which means only logical_file_names will be returned in the output json. 
	In order to get more information, one needs to provide detail=True.
	Run numbers must be passed as two parameters, minrun and maxrun. 
	for lumi_list the following two json formats are supported:
	    - '[a1, a2, a3,]' 
	    - '[[a,b], [c, d],]'
	Also if lumi_list is provided, one also needs to provide both minrun and maxrun parameters(equal) 
	No POST/PUT call for run-lumi json combination is provided as input for now...
        """
        logical_file_name = logical_file_name.replace("*", "%")
	release_version = release_version.replace("*", "%")
	pset_hash = pset_hash.replace("*", "%")
	app_name = app_name.replace("*", "%")
	block_name = block_name.replace("*", "%")
	origin_site_name = origin_site_name.replace("*", "%")
	dataset = dataset.replace("*", "%")
	maxrun = int(maxrun)
	minrun = int(minrun)
	if lumi_list:
	    #lumi_list = cjson.decode(lumi_list)
	    lumi_list = self.dbsUtils2.decodeLumiIntervals(lumi_list)
	detail = detail in (True, 1, "True", "1", 'true')
	output_module_label = output_module_label.replace("*", "%")
        try:
            return self.dbsFile.listFiles(dataset, block_name, logical_file_name , release_version , pset_hash, app_name, 
					output_module_label, maxrun, minrun, origin_site_name, lumi_list, detail)
        except Exception, ex:
            if "dbsException-7" in ex.args[0]:
                raise HTTPError(400, str(ex))
            else:
                msg = "%s DBSReaderModel/listFiles. %s\n. Exception trace: \n %s" \
                    %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
                self.logger.exception( msg )
                raise Exception ("dbsException-3", msg )

    @tools.secmodv2()
    def listFileSummaries(self, block_name='', dataset=''):
        """
        Example url's <br />
        http://dbs3/filesummaries?dataset=/a/b/c
        http://dbs3/filesummaries?block_name=/a/b/c#1234
        Both block_name and dataset will not allow wildcards.
        Return: number of files, event counts and number of lumi sections in a given block or dataset. 
        """
        try:
            return self.dbsFile.listFileSummary(block_name, dataset)
        except Exception, ex:
            if "dbsException-7" in ex.args[0]:
                raise HTTPError(400, str(ex))
            else:
                msg = "%s DBSReaderModel/listFileSummaries. %s\n. Exception trace: \n %s" \
                    %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
                self.logger.exception( msg )
                raise Exception ("dbsException-3", msg )

    @tools.secmodv2()
    def listDatasetParents(self, dataset=''):
        """
        Example url's <br />
        http://dbs3/datasetparents?dataset=/a/b/c
        """
        try:
            return self.dbsDataset.listDatasetParents(dataset)
        except Exception, ex:
            if "dbsException-7" in ex.args[0]:
                raise HTTPError(400, str(ex))
            else:
                msg = "%s DBSReaderModel/listDatasetParents. %s\n. Exception trace: \n %s" \
                    %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
                self.logger.exception( msg )
                raise Exception ("dbsException-3", msg )
   
    @tools.secmodv2()
    def listDatasetChildren(self, dataset):
        """
        Example url's <br />
        http://dbs3/datasetchildren?dataset=/a/b/c
        """
        try:
            return self.dbsDataset.listDatasetChildren(dataset)
        except Exception, ex:
            if "dbsException-7" in ex.args[0]:
                raise HTTPError(400, str(ex))
            else:
                msg = "%s DBSReaderModel/listDatasetChildren. %s\n. Exception trace: \n %s" \
                    %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
                self.logger.exception( msg )
                raise Exception ("dbsException-3", msg )
   
    @tools.secmodv2()
    def listOutputConfigs(self, dataset="", logical_file_name="", release_version="", pset_hash="", app_name="",
    output_module_label="", block_id=0, global_tag=''):
        """
        Example url's: <br />
        http://dbs3/outputconfigurations <br />
        http://dbs3/outputconfigurations?dataset=a/b/c <br />
        http://dbs3/outputconfigurations?logical_file_name=lfn <br />
        http://dbs3/outputconfigurations?release_version=version <br />
        http://dbs3/outputconfigurations?pset_hash=hash <br/>
        http://dbs3/outputconfigurations?app_name=app_name <br/>
        http://dbs3/outputconfigurations?output_module_label="output_module_label" <br/>
        """
       	release_version = release_version.replace("*", "%")
	pset_hash = pset_hash.replace("*", "%")
	app_name = app_name.replace("*", "%")
	output_module_label = output_module_label.replace("*", "%")
        try:
            return self.dbsOutputConfig.listOutputConfigs(dataset, logical_file_name, release_version, pset_hash, app_name,
                output_module_label, block_id, global_tag)
        except Exception, ex:
            if "dbsException-7" in ex.args[0]:
                raise HTTPError(400, str(ex))
            else:
                msg = "%s DBSReaderModel/listOutputConfigs. %s\n. Exception trace: \n %s" \
                    %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
                self.logger.exception( msg )
                raise Exception ("dbsException-3", msg )
    
    @tools.secmodv2()
    def listFileParents(self, logical_file_name='', block_id=0, block_name=''):
        """
        Example url's <br />
        http://dbs3/fileparents?logical_file_name=lfn
        """
        try:
            return self.dbsFile.listFileParents(logical_file_name, block_id, block_name)
        except Exception, ex:
            if "dbsException-7" in ex.args[0]:
                raise HTTPError(400, str(ex))
            else:
                msg = "%s DBSReaderModel/listFileParents. %s\n. Exception trace: \n %s" \
                    %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
                self.logger.exception( msg )
                raise Exception ("dbsException-3", msg )

    @tools.secmodv2()
    def listFileChildren(self, logical_file_name=''):
        """
        Example url's <br />
        http://dbs3/filechildren?logical_file_name=lfn
        """
        try:
            return self.dbsFile.listFileChildren(logical_file_name)
        except Exception, ex:
            if "dbsException-7" in ex.args[0]:
                raise HTTPError(400, str(ex))
            else:
                msg = "%s DBSReaderModel/listFileChildren. %s\n. Exception trace: \n %s" \
                    %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
                self.logger.exception( msg )
                raise Exception ("dbsException-3", msg )
    
    @tools.secmodv2()
    def listFileLumis(self, logical_file_name="", block_name="", run_num='0'):
        """
        Example url's <br />
        http://dbs3/filelumis?logical_file_name=lfn
        http://dbs3/filelumis?block_name=block_name
        """
        if '*' in run_num or '%' in run_num:
            run_num=0
        else:
            run_num = int(run_num)
        try:
            return self.dbsFile.listFileLumis(logical_file_name, block_name, run_num )
        except Exception, ex:
            if "dbsException-7" in ex.args[0]:
                raise HTTPError(400, str(ex))
            else:
                msg = "\n%s DBSReaderModel/listFileLumis. %s\n. Exception trace: \n %s" \
                    %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
                self.logger.exception( msg )
                raise Exception ("dbsException-3", msg )

    @tools.secmodv2()     
    def listRuns(self, minrun=-1, maxrun=-1, logical_file_name="", block_name="", dataset=""):
        """
        http://dbs3/runs?runmin=1&runmax=10
        http://dbs3/runs
        """
        try:
            if(logical_file_name):
                logical_file_name= logical_file_name.replace("*", "%")
                #print ("LFN=%s\n" %logical_file_name) 
            if(block_name):
                block_name = block_name.replace("*", "%")
                #print("Block=%s\n" %block_name)
            if(dataset):
                dataset = dataset.replace("*", "%")
                #print("ds=%s\n" %dataset)
            #print ("maxrun=%s, minrun=%s\n" %(maxrun, minrun) )
            return self.dbsRun.listRuns(minrun, maxrun, logical_file_name, block_name, dataset)
        except Exception, ex:
            if "dbsException-7" in ex.args[0]:
                raise HTTPError(400, str(ex))
            else:
                msg = "%s DBSReaderModel/listRun. %s\n. Exception trace: \n %s" \
                    %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
                self.logger.exception( msg )
                raise Exception ("dbsException-3", msg )

    #def listSites(self, block_name="", site_name=""):
    #    """
    #    Example url's <br />
    #    http://dbs3/sites
    #	http://dbs3/sites?block_name=block_name
    #	http://dbs3/sites?site_name=T1_FNAL
    #    """
    #    return self.dbsSite.listSites(block_name, site_name)

    @tools.secmodv2()
    def listDataTypes(self, datatype="", dataset=""):
	"""
	lists datatypes known to dbs
	dataset : lists datatype of a dataset
	"""
        try:
            return  self.dbsDataType.listDataType(dataType=datatype, dataset=dataset)
        except Exception, ex:
            if "dbsException-7" in ex.args[0]:
                raise HTTPError(400, str(ex))
            else:
                msg = "%s DBSReaderModel/listDataTypes. %s\n. Exception trace: \n %s" \
                    %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
                self.logger.exception( msg )
                raise Exception ("dbsException-3", msg )
    @tools.secmodv2()
    def dumpBlock(self, block_name):
	"""
	Returns all information related with the block_name
	"""
        try:
            return self.dbsMigrate.dumpBlock(block_name)
        except Exception, ex:
            if "dbsException-7" in ex.args[0]:
                raise HTTPError(400, str(ex))
            else:
                msg = "%s DBSReaderModel/listBlockParents. %s\n. Exception trace: \n %s" \
                    %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
                self.logger.exception( msg )
                raise Exception ("dbsException-3", msg )

    @tools.secmodv2()
    def listAcquisitionEras(self, acquisition_era_name=''):
	"""
	lists acquisition eras known to dbs
	"""
        try:
            acquisition_era_name = acquisition_era_name.replace('*', '%')
            return  self.dbsAcqEra.listAcquisitionEras(acquisition_era_name)
        except Exception, ex:
            if "dbsException-7" in ex.args[0]:
                raise HTTPError(400, str(ex))
            else:
                msg = "%s DBSReaderModel/listAcquisitionEras. %s\n. Exception trace: \n %s" \
                    %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
                self.logger.exception( msg )
                raise Exception ("dbsException-3", msg )

    @tools.secmodv2()
    def listProcessingEras(self, processing_version=''):
	"""
	lists acquisition eras known to dbs
	"""
        try:
            processing_version = processing_version.replace("*", "%")
            return  self.dbsProcEra.listProcessingEras(processing_version)
        except Exception, ex:
            if "dbsException-7" in ex.args[0]:
                raise HTTPError(400, str(ex))
            else:
                msg = "%s DBSReaderModel/listProcessingEras. %s\n. Exception trace: \n %s" \
                    %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
                self.logger.exception( msg )
                raise Exception ("dbsException-3", msg )

    @tools.secmodv2()
    def listReleaseVersions(self, release_version='', dataset=''):
        """
        lists release versions known to dbs
        """
        if release_version:
            release_version = release_version.replace("*","%")
        try:
            return  self.dbsReleaseVersion.listReleaseVersions(release_version,dataset )
        except Exception, ex:
            if "dbsException-7" in ex.args[0]:
                raise HTTPError(400, str(ex))
            else:
                msg = "%s DBSReaderModel/listReleaseVersions. %s\n. Exception trace: \n %s" \
                    %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
                self.logger.exception( msg )
                raise Exception ("dbsException-3", msg )
    
    @tools.secmodv2()
    def listDatasetAccessTypes(self, dataset_access_type=''):
        """
        lists dataset access types known to dbs
        """
        if dataset_access_type:
            dataset_access_type = dataset_access_type.replace("*","%")
        try:
            return  self.dbsDatasetAccessType.listDatasetAccessTypes(dataset_access_type)
        except Exception, ex:
            if "dbsException-7" in ex.args[0]:
                raise HTTPError(400, str(ex))
            else:
                msg = "%s DBSReaderModel/listDatasetAccessTypes. %s\n. Exception trace: \n %s" \
                    %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
                self.logger.exception( msg )
                raise Exception ("dbsException-3", msg )

    @tools.secmodv2()
    def listPhysicsGroups(self, physics_group_name=''):
        """
        List physics group names know to DBS.
        """
        if physics_group_name:
            physics_group_name = physics_group_name.replace('*', '%')
        try:
            return self.dbsPhysicsGroup.listPhysicsGroups(physics_group_name)
        except Exception, ex:
            if "dbsException-7" in ex.args[0]:
                raise HTTPError(400, str(ex))
            else:
                msg = "%s DBSReaderModel/listPhysicsGroups. %s\n. Exception trace: \n %s" \
                    %(DBSEXCEPTIONS['dbsException-3'], ex, traceback.format_exc())
                self.logger.exception( msg )
                raise Exception ("dbsException-3", msg )

