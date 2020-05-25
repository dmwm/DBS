from __future__ import with_statement
#!/usr/bin/env python
#pylint: disable=C0103
"""
DBS Reader Rest Model module
"""

__revision__ = "$Id: DBSReaderModel.py,v 1.50 2010/08/13 20:38:37 yuyi Exp $"
__version__ = "$Revision: 1.50 $"

import cjson
import re
import traceback

from cherrypy.lib import profiler
from cherrypy import request, tools, HTTPError
from WMCore.WebTools.RESTModel import RESTModel
from Utils.Throttled import UserThrottle
from dbs.utils.dbsUtils import jsonstreamer
from dbs.utils.dbsUtils import dbsUtils
from dbs.utils.DBSTransformInputType import parseRunRange
from dbs.business.DBSDoNothing import DBSDoNothing
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
from dbs.business.DBSBlockInsert import DBSBlockInsert
from dbs.business.DBSReleaseVersion import DBSReleaseVersion
from dbs.business.DBSDatasetAccessType import DBSDatasetAccessType
from dbs.business.DBSPhysicsGroup import DBSPhysicsGroup
from dbs.utils.dbsException import dbsException, dbsExceptionCode
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.DBSInputValidation import *
from dbs.utils.DBSTransformInputType import transformInputType, run_tuple
from WMCore.DAOFactory import DAOFactory


#Necessary for sphinx documentation and server side unit tests.
if not getattr(tools, "secmodv2", None):
    class FakeAuthForDoc(object):
        def __init__(self, *args, **kwargs):
            pass

        def callable(self, role=[], group=[], site=[], authzfunc=None):
            pass

    tools.secmodv2 = FakeAuthForDoc()

def authInsert(user, role, group, site):
    """
    Authorization function for general insert
    """
    if not role:
        return True
    for k, v in user['roles'].iteritems():
        for g in v['group']:
            if k in role.get(g, '').split(':'):
                return True
    return False

thr = UserThrottle()

class DBSReaderModel(RESTModel):
    """
    DBS3 Server API Documentation
    """
    def __init__(self, config):
        """
        All parameters are provided through DBSConfig module
        """
   
        #throllting setting
        thr.limit = config.database.throllting_limit

        #Dictionary with reader and writer as keys
        urls = config.database.connectUrl

        #instantiate the page with the writer_config
        if type(urls)==type({}):
            config.database.connectUrl = urls['reader']

        dbowner = config.database.dbowner

        RESTModel.__init__(self, config)

        # set proper logger name
        self.logger.name = __name__

        self.dbsUtils2 = dbsUtils()
        self.version = config.database.version
        self.instance = config.instance
        self.security_params = config.security.params
        self.methods = {'GET':{}, 'PUT':{}, 'POST':{}, 'DELETE':{}}

        self.daofactory = DAOFactory(package='dbs.dao', logger=self.logger, dbinterface=self.dbi, owner=dbowner)

        self.dbsDataTierListDAO = self.daofactory(classname="DataTier.List")
        self.dbsBlockSummaryListDAO = self.daofactory(classname="Block.SummaryList")
        self.dbsRunSummaryListDAO = self.daofactory(classname="Run.SummaryList")
        self.ParentDSFileLumiIdsDAO = self.daofactory(classname="FileParent.ListParentDatasetFileLumiIds")
        self.BKFileLumiIdsDAO = self.daofactory(classname="FileParent.ListBlockFileLumiIds")
        self._addMethod('GET', 'serverinfo', self.getServerInfo, secured=True,
                        security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('GET', 'primarydatasets', self.listPrimaryDatasets, args=['primary_ds_name', 'primary_ds_type'],
                        secured=True, security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('GET', 'primarydstypes', self.listPrimaryDsTypes, args=['primary_ds_type', 'dataset'],
                        secured=True, security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('GET', 'datasets', self.listDatasets, args=['dataset', 'parent_dataset', 'release_version',
                                'pset_hash', 'app_name', 'output_module_label', 'global_tag', 'processing_version',
                                'acquisition_era_name', 'run_num', 'physics_group_name', 'logical_file_name',
                                'primary_ds_name', 'primary_ds_type', 'processed_ds_name', 'data_tier_name',
                                'dataset_access_type', 'prep_id', 'create_by', 'last_modified_by',
                                'min_cdate', 'max_cdate', 'min_ldate', 'max_ldate', 'cdate', 'ldate', 'detail', 'dataset_id'],
                        secured=True, security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('POST', 'datasetlist', self.listDatasetArray, secured=True,
                        security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('GET', 'blocks', self.listBlocks, args=['dataset', 'block_name', 'data_tier_name',
                        'origin_site_name', 'logical_file_name', 'run_num', 'min_cdate', 'max_cdate', 'min_ldate',
                        'max_ldate', 'cdate', 'ldate', 'open_for_writing', 'detail'], secured=True,
                        security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('GET', 'blockorigin', self.listBlockOrigin, args=['origin_site_name', 'dataset', 'block_name'],
                        secured=True, security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('GET', 'files', self.listFiles, args=['dataset', 'block_name', 'logical_file_name',
                        'release_version', 'pset_hash', 'app_name', 'output_module_label', 'run_num',
                        'origin_site_name', 'lumi_list', 'detail', 'validFileOnly', 'sumOverLumi'], secured=True,
                        security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('POST', 'fileArray', self.listFileArray, secured=True,
                        security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('GET', 'filesummaries', self.listFileSummaries, args=['block_name', 'dataset',
                        'run_num', 'validFileOnly', 'sumOverLumi'], secured=True,
                        security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('GET', 'datasetparents', self.listDatasetParents, args=['dataset'], secured=True,
                        security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('GET', 'datasetchildren', self.listDatasetChildren, args=['dataset'], secured=True,
                        security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('GET', 'outputconfigs', self.listOutputConfigs, args=['dataset', 'logical_file_name',
                        'release_version', 'pset_hash', 'app_name', 'output_module_label', 'block_id', 'global_tag'],
                        secured=True, security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('GET', 'fileparents', self.listFileParents, args=['logical_file_name', 'block_id',
                        'block_name'], secured=True,
                        security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('POST', 'fileparentsbylumi', self.listFileParentsByLumi,
                        secured=True, security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('GET', 'blockTrio', self.listBlockTrio, args=['block_name'],
                        secured=True, security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('GET', 'parentDSTrio', self.listParentDSTrio, args=['dataset'], secured=True,
                        security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('GET', 'filechildren', self.listFileChildren, args=['logical_file_name', 'block_name', 'block_id'], secured=True,
                        security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('GET', 'filelumis', self.listFileLumis, args=['logical_file_name', 'block_name', 'run_num', 'validFileOnly'],
                        secured=True, security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('POST', 'filelumis', self.listFileLumiArray, secured=True, security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('GET', 'runs', self.listRuns, args=['run_num', 'logical_file_name',
                        'block_name', 'dataset'], secured=True,
                        security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('GET', 'datatypes', self.listDataTypes, args=['datatype', 'dataset'], secured=True,
                        security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('GET', 'datatiers', self.listDataTiers, args=['data_tier_name'], secured=True,
                        security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('GET', 'blockparents', self.listBlockParents, args=['block_name'], secured=True,
                        security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('POST', 'blockparents', self.listBlocksParents, secured=True,
                        security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('GET', 'blockchildren', self.listBlockChildren, args=['block_name'], secured=True,
                        security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('GET', 'blockdump', self.dumpBlock, args=['block_name'], secured=True,
                        security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('GET', 'blocksummaries', self.listBlockSummaries, args=['block_name', 'dataset', 'detail'], secured=True,
                        security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('GET', 'acquisitioneras', self.listAcquisitionEras, args=['acquisition_era_name'], secured=True,
                        security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('GET', 'acquisitioneras_ci', self.listAcquisitionEras_CI, args=['acquisition_era_name'],
                        secured=True, security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('GET', 'processingeras', self.listProcessingEras, args=['processing_version'], secured=True,
                        security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('GET', 'releaseversions', self.listReleaseVersions, args=['release_version', 'dataset',
                                                                                  'logical_file_name'], secured=True,
                        security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('GET', 'datasetaccesstypes', self.listDatasetAccessTypes, args=['dataset_access_type'],
                        secured=True, security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('GET', 'physicsgroups', self.listPhysicsGroups, args=['physics_group_name'], secured=True,
                        security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('GET', 'runsummaries', self.listRunSummaries, args=['dataset', 'run_num'], secured=True,
                        security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)
        self._addMethod('GET', 'help', self.getHelp, args=['call'], secured=True,
                        security_params={'role': self.security_params, 'authzfunc': authInsert}, dump_request_info=True)

        self.dbsDoNothing = DBSDoNothing(self.logger, self.dbi, dbowner)
        self.dbsPrimaryDataset = DBSPrimaryDataset(self.logger, self.dbi, dbowner)
        self.dbsDataset = DBSDataset(self.logger, self.dbi, dbowner)
        self.dbsBlock = DBSBlock(self.logger, self.dbi, dbowner)
        self.dbsFile = DBSFile(self.logger, self.dbi, dbowner)
        self.dbsAcqEra = DBSAcquisitionEra(self.logger, self.dbi, dbowner)
        self.dbsOutputConfig = DBSOutputConfig(self.logger, self.dbi, dbowner)
        self.dbsProcEra = DBSProcessingEra(self.logger, self.dbi, dbowner)
        self.dbsSite = DBSSite(self.logger, self.dbi, dbowner)
        self.dbsRun = DBSRun(self.logger, self.dbi, dbowner)
        self.dbsDataType = DBSDataType(self.logger, self.dbi, dbowner)
        self.dbsBlockInsert = DBSBlockInsert(self.logger, self.dbi, dbowner)
        self.dbsReleaseVersion = DBSReleaseVersion(self.logger, self.dbi, dbowner)
        self.dbsDatasetAccessType = DBSDatasetAccessType(self.logger, self.dbi, dbowner)
        self.dbsPhysicsGroup = DBSPhysicsGroup(self.logger, self.dbi, dbowner)

    def getHelp(self, call=""):
        """
        API to get a list of supported REST APIs. In the case a particular API is specified,
        the docstring of that API is displayed.

        :param call: call to get detailed information about (Optional)
        :type call: str
        :return: List of APIs or detailed information about a specific call (parameters and docstring)
        :rtype: List of strings or a dictionary containing params and doc keys depending on the input parameter

        """
        if call:
            params = self.methods['GET'][call]['args']
            doc = self.methods['GET'][call]['call'].__doc__
            return dict(params=params, doc=doc)
        else:
            return self.methods['GET'].keys()

    @thr.make_throttled()
    def getServerInfo(self):
        """
        Method that provides information about DBS Server to the clients
        The information includes

        :return: Server Version
        :rtype: dictionary containing dbs_version

        """
        return dict(dbs_version=self.version, dbs_instance=self.instance)

    @inputChecks(primary_ds_name=basestring, primary_ds_type=basestring)
    @thr.make_throttled()
    def listPrimaryDatasets(self, primary_ds_name="", primary_ds_type=""):
        """
        API to list primary datasets

        :param primary_ds_type: List primary datasets with primary dataset type (Optional)
        :type primary_ds_type: str
        :param primary_ds_name: List that primary dataset (Optional)
        :type primary_ds_name: str
        :returns: List of dictionaries containing the following keys (primary_ds_type_id, data_type)
        :rtype: list of dicts
        :returns: List of dictionaries containing the following keys (create_by, primary_ds_type, primary_ds_id, primary_ds_name, creation_date)
        :rtype: list of dicts

        """
        primary_ds_name = primary_ds_name.replace("*", "%")
        primary_ds_type = primary_ds_type.replace("*", "%")
        try:
            return self.dbsPrimaryDataset.listPrimaryDatasets(primary_ds_name, primary_ds_type)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except Exception as ex:
            sError = "DBSReaderModel/listPrimaryDatasets. %s\n Exception trace: \n %s." \
                    % (ex, traceback.format_exc() )
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(primary_ds_type=basestring, dataset=basestring)
    @thr.make_throttled()
    def listPrimaryDsTypes(self, primary_ds_type="", dataset=""):
        """
        API to list primary dataset types

        :param primary_ds_type: List that primary dataset type (Optional)
        :type primary_ds_type: str
        :param dataset: List the primary dataset type for that dataset (Optional)
        :type dataset: str
        :returns: List of dictionaries containing the following keys (primary_ds_type_id, data_type)
        :rtype: list of dicts

        """
        if primary_ds_type:
            primary_ds_type = primary_ds_type.replace("*", "%")
        if dataset:
            dataset = dataset.replace("*", "%")
        try:
            return self.dbsPrimaryDataset.listPrimaryDSTypes(primary_ds_type, dataset)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except Exception as ex:
            sError = "DBSReaderModel/listPrimaryDsTypes. %s\n. Exception trace: \n %s" \
                % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @transformInputType('run_num')
    @inputChecks( dataset=basestring, parent_dataset=basestring, release_version=basestring, pset_hash=basestring,
                 app_name=basestring, output_module_label=basestring, global_tag=basestring, processing_version=(int, basestring), acquisition_era_name=basestring,
                 run_num=(long, int, basestring, list), physics_group_name=basestring, logical_file_name=basestring, primary_ds_name=basestring,
                 primary_ds_type=basestring, processed_ds_name=basestring, data_tier_name=basestring, dataset_access_type=basestring, prep_id=basestring,
                 create_by=(basestring), last_modified_by=(basestring), min_cdate=(int, basestring), max_cdate=(int, basestring),
                 min_ldate=(int, basestring), max_ldate=(int, basestring), cdate=(int, basestring), ldate=(int, basestring), detail=(bool, basestring),
                 dataset_id=(int, long, basestring))
    @thr.make_throttled()
    def listDatasets(self, dataset="", parent_dataset="", is_dataset_valid=1,
        release_version="", pset_hash="", app_name="", output_module_label="", global_tag="",
        processing_version=0, acquisition_era_name="", run_num=-1,
        physics_group_name="", logical_file_name="", primary_ds_name="", primary_ds_type="",
        processed_ds_name='', data_tier_name="", dataset_access_type="VALID", prep_id='', create_by="", last_modified_by="",
        min_cdate='0', max_cdate='0', min_ldate='0', max_ldate='0', cdate='0',
        ldate='0', detail=False, dataset_id=-1):
        """
        API to list dataset(s) in DBS
        * You can use ANY combination of these parameters in this API
        * In absence of parameters, all valid datasets known to the DBS instance will be returned

        :param dataset:  Full dataset (path) of the dataset.
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
        :param global_tag: global_tag
        :type global_tag: str
        :param processing_version: Processing Version
        :type processing_version: str
        :param acquisition_era_name: Acquisition Era
        :type acquisition_era_name: str
        :param run_num: Specify a specific run number or range. Possible format are: run_num, 'run_min-run_max' or ['run_min-run_max', run1, run2, ...]. run_num=1 is not allowed.
        :type run_num: int,list,str
        :param physics_group_name: List only dataset having physics_group_name attribute
        :type physics_group_name: str
        :param logical_file_name: List dataset containing the logical_file_name
        :type logical_file_name: str
        :param primary_ds_name: Primary Dataset Name
        :type primary_ds_name: str
        :param primary_ds_type: Primary Dataset Type (Type of data, MC/DATA)
        :type primary_ds_type: str
        :param processed_ds_name: List datasets having this processed dataset name
        :type processed_ds_name: str
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
        :param min_cdate: Lower limit for the creation date (unixtime) (Optional)
        :type min_cdate: int, str
        :param max_cdate: Upper limit for the creation date (unixtime) (Optional)
        :type max_cdate: int, str
        :param min_ldate: Lower limit for the last modification date (unixtime) (Optional)
        :type min_ldate: int, str
        :param max_ldate: Upper limit for the last modification date (unixtime) (Optional)
        :type max_ldate: int, str
        :param cdate: creation date (unixtime) (Optional)
        :type cdate: int, str
        :param ldate: last modification date (unixtime) (Optional)
        :type ldate: int, str
        :param detail: List all details of a dataset
        :type detail: bool
        :param dataset_id: dataset table primary key used by CMS Computing Analytics.
        :type dataset_id: int, long, str
        :returns: List of dictionaries containing the following keys (dataset). If the detail option is used. The dictionary contain the following keys (primary_ds_name, physics_group_name, acquisition_era_name, create_by, dataset_access_type, data_tier_name, last_modified_by, creation_date, processing_version, processed_ds_name, xtcrosssection, last_modification_date, dataset_id, dataset, prep_id, primary_ds_type)
        :rtype: list of dicts

        """
        dataset = dataset.replace("*", "%")
        parent_dataset = parent_dataset.replace("*", "%")
        release_version = release_version.replace("*", "%")
        pset_hash = pset_hash.replace("*", "%")
        app_name = app_name.replace("*", "%")
        output_module_label = output_module_label.replace("*", "%")
        global_tag = global_tag.replace("*", "%")
        logical_file_name = logical_file_name.replace("*", "%")
        physics_group_name = physics_group_name.replace("*", "%")
        primary_ds_name = primary_ds_name.replace("*", "%")
        primary_ds_type = primary_ds_type.replace("*", "%")
        data_tier_name = data_tier_name.replace("*", "%")
        dataset_access_type = dataset_access_type.replace("*", "%")
        processed_ds_name = processed_ds_name.replace("*", "%")
        acquisition_era_name = acquisition_era_name.replace("*", "%")
        #processing_version =  processing_version.replace("*", "%")
        #create_by and last_modified_by have be full spelled, no wildcard will allowed.
        #We got them from request head so they can be either HN account name or DN.
        #This is depended on how an user's account is set up.
        #
        # In the next release we will require dataset has no wildcard in it. 
        # DBS will reject wildcard search with dataset name with listDatasets call. 
        # One should seperate the dataset into primary , process and datatier if any wildcard.
        # YG Oct 26, 2016
        # Some of users were overwhiled by the API change. So we split the wildcarded dataset in the server instead of by the client.
        # YG Dec. 9 2016
        #
        # run_num=1 caused full table scan and CERN DBS reported some of the queries ran more than 50 hours
        # We will disbale all the run_num=1 calls in DBS. Run_num=1 will be OK when logical_file_name is given.
        # YG Jan. 15 2019
        #
        if (run_num != -1 and logical_file_name ==''):
            for r in parseRunRange(run_num):
                if isinstance(r, basestring) or isinstance(r, int) or isinstance(r, long):    
                    if r == 1 or r == '1':
                        dbsExceptionHandler("dbsException-invalid-input", "Run_num=1 is not a valid input.",
                                self.logger.exception)
                elif isinstance(r, run_tuple):
                    if r[0] == r[1]:
                        dbsExceptionHandler('dbsException-invalid-input', "DBS run range must be apart at least by 1.", 
                          self.logger.exception)
                    elif r[0] <= 1 <= r[1]:
                        dbsExceptionHandler("dbsException-invalid-input", "Run_num=1 is not a valid input.",
                                self.logger.exception)     

        if( dataset and ( dataset == "/%/%/%" or dataset== "/%" or dataset == "/%/%" ) ):
            dataset=''
        elif( dataset and ( dataset.find('%') != -1 ) ) :
            junk, primary_ds_name, processed_ds_name, data_tier_name = dataset.split('/')
            dataset = ''
        if ( primary_ds_name == '%' ):
            primary_ds_name = ''
        if( processed_ds_name == '%' ):
            processed_ds_name = ''
        if ( data_tier_name == '%' ):
            data_tier_name = ''

        try:
            dataset_id = int(dataset_id)
        except:
            dbsExceptionHandler("dbsException-invalid-input2", "Invalid Input for dataset_id that has to be an int.",
                                self.logger.exception, 'dataset_id has to be an int.')
        if create_by.find('*')!=-1 or create_by.find('%')!=-1 or last_modified_by.find('*')!=-1\
                or last_modified_by.find('%')!=-1:
            dbsExceptionHandler("dbsException-invalid-input2", "Invalid Input for create_by or last_modified_by.\
            No wildcard allowed.",  self.logger.exception, 'No wildcards allowed for create_by or last_modified_by')
        try:
            if isinstance(min_cdate, basestring) and ('*' in min_cdate or '%' in min_cdate):
                min_cdate = 0
            else:
                try:
                    min_cdate = int(min_cdate)
                except:
                    dbsExceptionHandler("dbsException-invalid-input", "invalid input for min_cdate")
            
            if isinstance(max_cdate, basestring) and ('*' in max_cdate or '%' in max_cdate):
                max_cdate = 0
            else:
                try:
                    max_cdate = int(max_cdate)
                except:
                    dbsExceptionHandler("dbsException-invalid-input", "invalid input for max_cdate")
            
            if isinstance(min_ldate, basestring) and ('*' in min_ldate or '%' in min_ldate):
                min_ldate = 0
            else:
                try:
                    min_ldate = int(min_ldate)
                except:
                    dbsExceptionHandler("dbsException-invalid-input", "invalid input for min_ldate")
            
            if isinstance(max_ldate, basestring) and ('*' in max_ldate or '%' in max_ldate):
                max_ldate = 0
            else:
                try:
                    max_ldate = int(max_ldate)
                except:
                    dbsExceptionHandler("dbsException-invalid-input", "invalid input for max_ldate")
            
            if isinstance(cdate, basestring) and ('*' in cdate or '%' in cdate):
                cdate = 0
            else:
                try:
                    cdate = int(cdate)
                except:
                    dbsExceptionHandler("dbsException-invalid-input", "invalid input for cdate")
            
            if isinstance(ldate, basestring) and ('*' in ldate or '%' in ldate):
                ldate = 0
            else:
                try:
                    ldate = int(ldate)
                except:
                    dbsExceptionHandler("dbsException-invalid-input", "invalid input for ldate")
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception as ex:
            sError = "DBSReaderModel/listDatasets.  %s \n. Exception trace: \n %s" \
                % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

        detail = detail in (True, 1, "True", "1", 'true')
        try: 
            return self.dbsDataset.listDatasets(dataset, parent_dataset, is_dataset_valid, release_version, pset_hash,
                app_name, output_module_label, global_tag, processing_version, acquisition_era_name, 
                run_num, physics_group_name, logical_file_name, primary_ds_name, primary_ds_type, processed_ds_name,
                data_tier_name, dataset_access_type, prep_id, create_by, last_modified_by,
                min_cdate, max_cdate, min_ldate, max_ldate, cdate, ldate, detail, dataset_id)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception as ex:
            sError = "DBSReaderModel/listdatasets. %s.\n Exception trace: \n %s" % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)
    
    @thr.make_throttled()
    def listDatasetArray(self):
        """
        API to list datasets in DBS. To be called by datasetlist url with post call.

        :param dataset: list of datasets [dataset1,dataset2,..,dataset n] (must have either a list of dataset or dataset_id), Max length 1000.
        :type dataset: list
	:param dataset_id: list of dataset ids [dataset_id1,dataset_id2,..,dataset_idn, "dsid_min-dsid_max"] ((must have either a list of dataset or dataset_id)
        :type dataset_id: list
        :param dataset_access_type: List only datasets with that dataset access type (Optional)
        :type dataset_access_type: str
        :param detail: brief list or detailed list 1/0
        :type detail: bool
        :returns: List of dictionaries containing the following keys (dataset). If the detail option is used. The dictionary contains the following keys (primary_ds_name, physics_group_name, acquisition_era_name, create_by, dataset_access_type, data_tier_name, last_modified_by, creation_date, processing_version, processed_ds_name, xtcrosssection, last_modification_date, dataset_id, dataset, prep_id, primary_ds_type)
        :rtype: list of dicts

        """
        ret = []
        try :
            body = request.body.read()
            if body:
                data = cjson.decode(body)
                data = validateJSONInputNoCopy("dataset", data, read=True)
                #Because CMSWEB has a 300 seconds responding time. We have to limit the array siz to make sure that
                #the API can be finished in 300 second. 
                # YG Nov-05-2015
                max_array_size = 1000
                if ( 'dataset' in data.keys() and isinstance(data['dataset'], list) and len(data['dataset'])>max_array_size)\
                    or ('dataset_id' in data.keys() and isinstance(data['dataset_id'], list) and len(data['dataset_id'])>max_array_size):
                    dbsExceptionHandler("dbsException-invalid-input",
                                        "The Max list length supported in listDatasetArray is %s." %max_array_size, self.logger.exception)    
                ret = self.dbsDataset.listDatasetArray(data)
        except cjson.DecodeError as De:
            dbsExceptionHandler('dbsException-invalid-input2', "Invalid input", self.logger.exception, str(De))
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except HTTPError as he:
            raise he
        except Exception as ex:
            sError = "DBSReaderModel/listDatasetArray. %s \n Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)
        for item in ret:
                    yield item

    @inputChecks(data_tier_name=basestring)
    @thr.make_throttled()
    def listDataTiers(self, data_tier_name=""):
        """
        API to list data tiers known to DBS.

        :param data_tier_name: List details on that data tier (Optional)
        :type data_tier_name: str
        :returns: List of dictionaries containing the following keys (data_tier_id, data_tier_name, create_by, creation_date)

        """
        data_tier_name = data_tier_name.replace("*", "%")

        try:
            conn = self.dbi.connection()
            return self.dbsDataTierListDAO.execute(conn, data_tier_name.upper())
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except ValueError as ve:
            dbsExceptionHandler("dbsException-invalid-input2", "Invalid Input Data",  self.logger.exception, ve.message)
        except TypeError as te:
            dbsExceptionHandler("dbsException-invalid-input2", "Invalid Input DataType",  self.logger.exception, te.message)
        except NameError as ne:
            dbsExceptionHandler("dbsException-invalid-input2", "Invalid Input Searching Key",  self.logger.exception, ne.message)
        except Exception as ex:
            sError = "DBSReaderModel/listDataTiers. %s\n. Exception trace: \n %s" \
                    % ( ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)
        finally:
            if conn:
                conn.close()

    @transformInputType('run_num')
    @inputChecks(dataset=basestring, block_name=basestring, data_tier_name=basestring, origin_site_name=basestring, logical_file_name=basestring,
                 run_num=(long, int, basestring, list), min_cdate=(int, basestring), max_cdate=(int, basestring), min_ldate=(int, basestring),
                 max_ldate=(int, basestring), cdate=(int, basestring),  ldate=(int, basestring), open_for_writing=(int, basestring), detail=(basestring, bool))
    @thr.make_throttled()
    def listBlocks(self, dataset="", block_name="", data_tier_name="", origin_site_name="",
                   logical_file_name="",run_num=-1, min_cdate='0', max_cdate='0',
                   min_ldate='0', max_ldate='0', cdate='0',  ldate='0', open_for_writing=-1, detail=False):

        """
        API to list a block in DBS. At least one of the parameters block_name, dataset, data_tier_name or
        logical_file_name are required. If data_tier_name is provided, min_cdate and max_cdate have to be specified and
        the difference in time have to be less than 31 days.

        :param block_name: name of the block
        :type block_name: str
        :param dataset: dataset
        :type dataset: str
        :param data_tier_name: data tier
        :type data_tier_name: str
        :param logical_file_name: Logical File Name
        :type logical_file_name: str
        :param origin_site_name: Origin Site Name (Optional)
        :type origin_site_name: str
        :param open_for_writing: Open for Writting (Optional)
        :type open_for_writing: int (0 or 1)
        :param run_num: run_num numbers (Optional). Possible format are: run_num, 'run_min-run_max' or ['run_min-run_max', run1, run2, ...].
        :type run_num: int, list of runs or list of run ranges
        :param min_cdate: Lower limit for the creation date (unixtime) (Optional)
        :type min_cdate: int, str
        :param max_cdate: Upper limit for the creation date (unixtime) (Optional)
        :type max_cdate: int, str
        :param min_ldate: Lower limit for the last modification date (unixtime) (Optional)
        :type min_ldate: int, str
        :param max_ldate: Upper limit for the last modification date (unixtime) (Optional)
        :type max_ldate: int, str
        :param cdate: creation date (unixtime) (Optional)
        :type cdate: int, str
        :param ldate: last modification date (unixtime) (Optional)
        :type ldate: int, str
        :param detail: Get detailed information of a block (Optional)
        :type detail: bool
        :returns: List of dictionaries containing following keys (block_name). If option detail is used the dictionaries contain the following keys (block_id, create_by, creation_date, open_for_writing, last_modified_by, dataset, block_name, file_count, origin_site_name, last_modification_date, dataset_id and block_size)
        :rtype: list of dicts

        """
        # run_num=1 caused full table scan and CERN DBS reported some of the queries ran more than 50 hours
        # We will disbale all the run_num=1 calls in DBS. Run_num=1 will be OK while logical_file_name is given.
        # YG Jan. 15 2019
        # 
        if (run_num != -1 and logical_file_name ==''):
            for r in parseRunRange(run_num):
                if isinstance(r, basestring) or isinstance(r, int) or isinstance(r, long):    
                    if r == 1 or r == '1':
                        dbsExceptionHandler("dbsException-invalid-input", "Run_num=1 is not a valid input.",
                                self.logger.exception)
                elif isinstance(r, run_tuple):
                    if r[0] == r[1]:
                        dbsExceptionHandler("dbsException-invalid-input", "DBS run range must be apart at least by 1.",
                          self.logger.exception)
                    elif r[0] <= 1 <= r[1]:
                        dbsExceptionHandler("dbsException-invalid-input2", "Run_num=1 is not a valid input.",
                                self.logger.exception)
        
        dataset = dataset.replace("*", "%")
        block_name = block_name.replace("*", "%")
        logical_file_name = logical_file_name.replace("*", "%")
        origin_site_name = origin_site_name.replace("*", "%")
        #
	if isinstance(min_cdate, basestring) and ('*' in min_cdate or '%' in min_cdate):
            min_cdate = 0
        else:
            try:
                min_cdate = int(min_cdate)
            except:
                dbsExceptionHandler("dbsException-invalid-input", "invalid input for min_cdate")
        #
        if isinstance(max_cdate, basestring) and ('*' in max_cdate or '%' in max_cdate):
            max_cdate = 0
        else:
            try:
                max_cdate = int(max_cdate)
            except:
                dbsExceptionHandler("dbsException-invalid-input", "invalid input for max_cdate")
        #
        if isinstance(min_ldate, basestring) and ('*' in min_ldate or '%' in min_ldate):
            min_ldate = 0
        else:
            try:
                min_ldate = int(min_ldate)
            except:
                dbsExceptionHandler("dbsException-invalid-input", "invalid input for max_cdate")
        #
	if isinstance(max_ldate, basestring) and ('*' in max_ldate or '%' in max_ldate):
            max_ldate = 0
        else:
            try:
                max_ldate = int(max_ldate)
            except:
                dbsExceptionHandler("dbsException-invalid-input", "invalid input for max_ldate")
        #
        if isinstance(cdate, basestring) and ('*' in cdate or '%' in cdate):
            cdate = 0
        else:
            try:
                cdate = int(cdate)
            except:
                dbsExceptionHandler("dbsException-invalid-input", "invalid input for cdate")
        #
        if isinstance(cdate, basestring) and ('*' in ldate or '%' in ldate):
            ldate = 0
        else:
            try:
                ldate = int(ldate)
            except:
                dbsExceptionHandler("dbsException-invalid-input", "invalid input for ldate")
        #
        detail = detail in (True, 1, "True", "1", 'true')
        try:
            b= self.dbsBlock.listBlocks(dataset, block_name, data_tier_name, origin_site_name, logical_file_name,
                                  run_num, min_cdate, max_cdate, min_ldate, max_ldate, cdate, ldate, open_for_writing, detail)
	    #for item in b:
		#yield item
	    return b	
	except HTTPError:
	    raise	
	except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception as ex:
            sError = "DBSReaderModel/listBlocks. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(origin_site_name=basestring, dataset=basestring, block_name=basestring)
    @thr.make_throttled()
    def listBlockOrigin(self, origin_site_name="",  dataset="", block_name=""):
        """
        API to list blocks first generated in origin_site_name.

        :param origin_site_name: Origin Site Name (Optional, No wildcards)
        :type origin_site_name: str
        :param dataset: dataset ( No wildcards, either dataset or block name needed)
        :type dataset: str
        :param block_name:
        :type block_name: str
        :returns: List of dictionaries containing the following keys (create_by, creation_date, open_for_writing, last_modified_by, dataset, block_name, file_count, origin_site_name, last_modification_date, block_size)
        :rtype: list of dicts

        """
        try:
            return self.dbsBlock.listBlocksOrigin(origin_site_name, dataset, block_name)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception as ex:
            sError = "DBSReaderModel/listBlocks. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'],
                                self.logger.exception, sError)


    @inputChecks(block_name=basestring)
    @thr.make_throttled()
    def listBlockParents(self, block_name=""):
        """
        API to list block parents.

        :param block_name: name of block who's parents needs to be found (Required)
        :type block_name: str
        :returns: List of dictionaries containing following keys (block_name)
        :rtype: list of dicts

        """
        try:
            return self.dbsBlock.listBlockParents(block_name)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception as ex:
            sError = "DBSReaderModel/listBlockParents. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'],  self.logger.exception, sError)

    @thr.make_throttled()
    def listBlocksParents(self):
        """
        API to list block parents of multiple blocks. To be called by blockparents url with post call.

        :param block_names: list of block names [block_name1, block_name2, ...] (Required). Mwx length 1000.
        :type block_names: list

        """
        try :
            body = request.body.read()
            data = cjson.decode(body)
            data = validateJSONInputNoCopy("block", data, read=True)
            #Because CMSWEB has a 300 seconds responding time. We have to limit the array siz to make sure that
            #the API can be finished in 300 second. 
            # YG Nov-05-2015
            max_array_size = 1000
            if ( 'block_names' in data.keys() and isinstance(data['block_names'], list) and len(data['block_names'])>max_array_size):
                    dbsExceptionHandler("dbsException-invalid-input",
                                        "The Max list length supported in listBlocksParents is %s." %max_array_size, self.logger.exception)
            return self.dbsBlock.listBlockParents(data["block_name"])
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except cjson.DecodeError as de:
            sError = "DBSReaderModel/listBlockParents. %s\n. Exception trace: \n %s" \
                    % (de, traceback.format_exc())
            msg = "DBSReaderModel/listBlockParents. %s" % de
            dbsExceptionHandler('dbsException-invalid-input2', msg, self.logger.exception, sError)
        except HTTPError as he:
            raise he
        except Exception as ex:
            sError = "DBSReaderModel/listBlockParents. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(block_name=basestring)
    @thr.make_throttled()
    def listBlockChildren(self, block_name=""):
        """
        API to list block children.

        :param block_name: name of block who's children needs to be found (Required)
        :type block_name: str
        :returns: List of dictionaries containing following keys (block_name)
        :rtype: list of dicts

        """
        block_name = block_name.replace("*", "%")
        try:
            return self.dbsBlock.listBlockChildren(block_name)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception as ex:
            sError = "DBSReaderModel/listBlockChildren. %s\n. Exception trace: \n %s" % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @transformInputType('block_name')
    @inputChecks(block_name=(basestring, list), dataset=basestring, detail=(bool, basestring))
    @thr.make_throttled()
    def listBlockSummaries(self, block_name="", dataset="", detail=False):
        """
        API that returns summary information like total size and total number of events in a dataset or a list of blocks

        :param block_name: list block summaries for block_name(s)
        :type block_name: str, list
        :param dataset: list block summaries for all blocks in dataset
        :type dataset: str
        :param detail: list summary by block names if detail=True, default=False
        :type detail: str, bool
        :returns: list of dicts containing total block_sizes, file_counts and event_counts of dataset or blocks provided

        """
        if bool(dataset)+bool(block_name)!=1:
            dbsExceptionHandler("dbsException-invalid-input2",
                                dbsExceptionCode["dbsException-invalid-input2"],
                                self.logger.exception,
                                "Dataset or block_names must be specified at a time.")

        if block_name and isinstance(block_name, basestring):
            try:
                block_name = [str(block_name)]
            except:
                dbsExceptionHandler("dbsException-invalid-input", "Invalid block_name for listBlockSummaries. ")

        for this_block_name in block_name:
            if re.search("[*, %]", this_block_name):
                dbsExceptionHandler("dbsException-invalid-input2",
                                    dbsExceptionCode["dbsException-invalid-input2"],
                                    self.logger.exception,
                                    "No wildcards are allowed in block_name list")

        if re.search("[*, %]", dataset):
            dbsExceptionHandler("dbsException-invalid-input2",
                                dbsExceptionCode["dbsException-invalid-input2"],
                                self.logger.exception,
                                "No wildcards are allowed in dataset")
        data = [] 
        try:
            with self.dbi.connection() as conn:
                data = self.dbsBlockSummaryListDAO.execute(conn, block_name, dataset, detail)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception as ex:
            sError = "DBSReaderModel/listBlockSummaries. %s\n. Exception trace: \n %s" % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error',
                                dbsExceptionCode['dbsException-server-error'],
                                self.logger.exception,
                                sError)
        for item in data:
                    yield item

    @transformInputType( 'run_num')
    @inputChecks(dataset =basestring, block_name=basestring, logical_file_name =(basestring), release_version=basestring, pset_hash=basestring, app_name=basestring,\
                 output_module_label=basestring, run_num=(long, int, basestring, list), origin_site_name=basestring,
                 lumi_list=(basestring, list), detail=(basestring, bool), validFileOnly=(basestring, int), 
                 sumOverLumi=(basestring, int))
    @thr.make_throttled()
    def listFiles(self, dataset = "", block_name = "", logical_file_name = "",
        release_version="", pset_hash="", app_name="", output_module_label="",
        run_num=-1, origin_site_name="", lumi_list="", detail=False, validFileOnly=0, sumOverLumi=0):
        """
        API to list files in DBS. Either non-wildcarded logical_file_name, non-wildcarded dataset or non-wildcarded block_name is required.
        The combination of a non-wildcarded dataset or block_name with an wildcarded logical_file_name is supported.

        * For lumi_list the following two json formats are supported:
            - [a1, a2, a3,]
            - [[a,b], [c, d],]
	* lumi_list can be either a list of lumi section numbers as [a1, a2, a3,] or a list of lumi section range as [[a,b], [c, d],]. Thay cannot be mixed.
        * If lumi_list is provided run only run_num=single-run-number is allowed
	* When lfn list is present, no run or lumi list is allowed.

        *There are five dataset access types: VALID, INVALID, PRODUCTION, DEPRECATED and DELETED. 
        * One file status: IS_FILE_VALID: 1 or 0.
        * When a dataset is INVALID/ DEPRECATED/ DELETED, DBS will consider all the files under it is invalid not matter what value is_file_valid has. 
          In general, when the dataset is in one of INVALID/ DEPRECATED/ DELETED, is_file_valid should all marked as 0, but some old DBS2 data was not.
        * When Dataset is VALID/PRODUCTION, by default is_file_valid is all 1. But if individual file is invalid, then the file's is_file_valid is set to 0.
        * DBS use this logical in its APIs that have validFileOnly variable.
  
        *There are five dataset access types: VALID, INVALID, PRODUCTION, DEPRECATED and DELETED. 
        * One file status: IS_FILE_VALID: 1 or 0.
        * When a dataset is INVALID/ DEPRECATED/ DELETED, DBS will consider all the files under it is invalid not matter what value is_file_valid has. 
          In general, when the dataset is in one of INVALID/ DEPRECATED/ DELETED, is_file_valid should all marked as 0, but some old DBS2 data was not.
        * When Dataset is VALID/PRODUCTION, by default is_file_valid is all 1. But if individual file is invalid, then the file's is_file_valid is set to 0.
        * DBS use this logical in its APIs that have validFileOnly variable.

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
        :param run_num: run , run ranges, and run list. Possible format are: run_num, 'run_min-run_max' or ['run_min-run_max', run1, run2, ...].
        :type run_num: int, list, string
        :param origin_site_name: site where the file was created
        :type origin_site_name: str
        :param lumi_list: List containing luminosity sections
        :type lumi_list: list
        :param detail: Get detailed information about a file
        :type detail: bool
        :param validFileOnly: default=0 return all the files. when =1, only return files with is_file_valid=1 or dataset_access_type=PRODUCTION or VALID
        :type validFileOnly: int
        :param sumOverLumi: default=0 event_count is the event_count/file.  When sumOverLumi=1 and run_num is specified, the event_count is sum of the event_count/lumi for that run; When sumOverLumi = 1, no other input can be a list, for example no run_num list, lumi list or lfn list.
        :type sumOverLumi: int
        :returns: List of dictionaries containing the following keys (logical_file_name). If detail parameter is true, the dictionaries contain the following keys (check_sum, branch_hash_id, adler32, block_id, event_count, file_type, create_by, logical_file_name, creation_date, last_modified_by, dataset, block_name, file_id, file_size, last_modification_date, dataset_id, file_type_id, auto_cross_section, md5, is_file_valid)
        :rtype: list of dicts

        """
        logical_file_name = logical_file_name.replace("*", "%")
        release_version = release_version.replace("*", "%")
        pset_hash = pset_hash.replace("*", "%")
        app_name = app_name.replace("*", "%")
        block_name = block_name.replace("*", "%")
        origin_site_name = origin_site_name.replace("*", "%")
        dataset = dataset.replace("*", "%")
        #
        # run_num=1 caused full table scan and CERN DBA reported some of the queries ran more than 50 hours
        # We will disbale all the run_num=1 calls in DBS. Run_num=1 will be OK when logical_file_name is given.
        # YG Jan. 15 2019
        #
        if (run_num != -1 and logical_file_name ==''):
            for r in parseRunRange(run_num):
                if isinstance(r, basestring) or isinstance(r, int) or isinstance(r, long):    
                    if r == 1 or r == '1':
                        dbsExceptionHandler("dbsException-invalid-input", "While Run_num=1, LFN is required.",
                                self.logger.exception)
                elif isinstance(r, run_tuple):
                    if r[0] == r[1]:
                        dbsExceptionHandler("dbsException-invalid-input", "DBS run range must be apart at least by 1.",
                          self.logger.exception)
                    elif r[0] <= 1 <= r[1]:
                        dbsExceptionHandler("dbsException-invalid-input", "While Run_num includes 1, LFN is required.",
                                self.logger.exception)
        if lumi_list:
            if run_num ==-1 or not run_num :
                dbsExceptionHandler("dbsException-invalid-input", "When lumi_list is given, require a single run_num.", self.logger.exception)
            elif sumOverLumi == 1:
                 dbsExceptionHandler("dbsException-invalid-input", "lumi_list and sumOverLumi=1 cannot be set at the same time becaue nesting of WITH clause within WITH clause not supported yet by Oracle. ", self.logger.exception)
            else:
                try:
                    lumi_list = self.dbsUtils2.decodeLumiIntervals(lumi_list)
                except Exception as de:
                    dbsExceptionHandler("dbsException-invalid-input", "Invalid lumi_list input: "+ str(de), self.logger.exception)
        else:
            if not isinstance(run_num, list):
                if run_num ==1 or run_num == '1':
                    dbsExceptionHandler("dbsException-invalid-input", "While run_num=1, lumi and LFN are required.", self.logger.exception)
            else:
                if 1 in run_num or '1' in run_num :
                 dbsExceptionHandler("dbsException-invalid-input", "While run_num includes 1, lumi and LFN are required. ", self.logger.exception)
        if int(sumOverLumi) == 1 and (isinstance(run_num, list) or isinstance(logical_file_name, list)):
            dbsExceptionHandler("dbsException-invalid-input", "When sumOverLumi=1, no lfn list or run_num list allowed  becaue nesting of WITH clause within WITH clause not supported yet by Oracle. ", self.logger.exception)
        detail = detail in (True, 1, "True", "1", 'true')
        output_module_label = output_module_label.replace("*", "%")
        try:
            result =  self.dbsFile.listFiles(dataset, block_name, logical_file_name, release_version, pset_hash, app_name,
                                        output_module_label, run_num, origin_site_name, lumi_list, detail, 
                                        validFileOnly, sumOverLumi)
    	    for item in result:
		yield item	
	except HTTPError as he:
	    raise he
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception as ex:
            sError = "DBSReaderModel/listFiles. %s \n Exception trace: \n %s" % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', ex.message,
                    self.logger.exception, sError)

    @thr.make_throttled()
    def listFileArray(self):
        """
        API to list files in DBS. Either non-wildcarded logical_file_name, non-wildcarded dataset, 
	non-wildcarded block_name or non-wildcarded lfn list is required.
        The combination of a non-wildcarded dataset or block_name with an wildcarded logical_file_name is supported.
        
        * For lumi_list the following two json formats are supported:
            - [a1, a2, a3,]
            - [[a,b], [c, d],]
	* lumi_list can be either a list of lumi section numbers as [a1, a2, a3,] or a list of lumi section range as [[a,b], [c, d],]. Thay cannot be mixed.
        * If lumi_list is provided run only run_num=single-run-number is allowed
	* When lfn list is present, no run or lumi list is allowed.
        * When run_num =1 is present, logical_file_name should be present too.

        :param logical_file_name: logical_file_name of the file
        :type logical_file_name: str,  list
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
        :param run_num: run , run ranges, and run list. Possible format are: run_num, 'run_min-run_max' or ['run_min-run_max', run1, run2, ...]. Max length 1000.
        :type run_num: int, list, string
        :param origin_site_name: site where the file was created
        :type origin_site_name: str
        :param lumi_list: List containing luminosity sections. Max length 1000.
        :type lumi_list: list
        :param detail: Get detailed information about a file
        :type detail: bool
        :param validFileOnly: default=0 return all the files. when =1, only return files with is_file_valid=1 or dataset_access_type=PRODUCTION or VALID
        :type validFileOnly: int
        :param sumOverLumi: default=0 event_count is the event_count/file, when=1 and run_num is specified, the event_count is sum of the event_count/lumi for that run; When sumOverLumi = 1, no other input can be a list, for example no run_num list, lumi list or lfn list.
        :type sumOverLumi: int 
        :returns: List of dictionaries containing the following keys (logical_file_name). If detail parameter is true, the dictionaries contain the following keys (check_sum, branch_hash_id, adler32, block_id, event_count, file_type, create_by, logical_file_name, creation_date, last_modified_by, dataset, block_name, file_id, file_size, last_modification_date, dataset_id, file_type_id, auto_cross_section, md5, is_file_valid)
        :rtype: list of dicts

        """
        ret = []
        try :
            body = request.body.read()
            if body:
                data = cjson.decode(body)
                data = validateJSONInputNoCopy("files", data, True)
                if 'sumOverLumi' in data and data['sumOverLumi'] ==1:
                    if ('logical_file_name' in data and isinstance(data['logical_file_name'], list)) \
                       or ('run_num' in data and isinstance(data['run_num'], list)):
                        dbsExceptionHandler("dbsException-invalid-input",
                                            "When sumOverLumi=1, no input can be a list becaue nesting of WITH clause within WITH clause not supported yet by Oracle. ", self.logger.exception)
    
                if 'lumi_list' in data and data['lumi_list']:
                    if 'sumOverLumi' in data and data['sumOverLumi'] ==1:
                        dbsExceptionHandler("dbsException-invalid-input", 
                                            "When lumi_list is given, sumOverLumi must set to 0 becaue nesting of WITH clause within WITH clause not supported yet by Oracle.", self.logger.exception)
                    data['lumi_list'] = self.dbsUtils2.decodeLumiIntervals(data['lumi_list'])	
                    if 'run_num' not in data.keys() or not data['run_num'] or data['run_num'] ==-1 :
                        dbsExceptionHandler("dbsException-invalid-input", 
                                            "When lumi_list is given, require a single run_num.", self.logger.exception)
                #check if run_num =1 w/o lfn 
                if ('logical_file_name' not in data or not data['logical_file_name']) and 'run_num' in data:
                    if isinstance(data['run_num'], list):
                        if 1 in data['run_num'] or '1' in data['run_num']:
                            raise dbsExceptionHandler("dbsException-invalid-input",
                                  'While run_num=1, LFN is required.', self.logger.exception)
                        else:
                            if data['run_num'] == 1 or data['run_num'] == '1':
                                raise dbsExceptionHandler("dbsException-invalid-input",
                                   'While run_num=1, LFN is required.', self.logger.exception)                
                #Because CMSWEB has a 300 seconds responding time. We have to limit the array siz to make sure that
                #the API can be finished in 300 second. See github issues #465 for tests' results.
                # YG May-20-2015
                max_array_size = 1000
                if ( 'run_num' in data.keys() and isinstance(data['run_num'], list) and len(data['run_num'])>max_array_size)\
                    or ('lumi_list' in data.keys() and isinstance(data['lumi_list'], list) and len(data['lumi_list'])>max_array_size)\
                    or ('logical_file_name' in data.keys() and isinstance(data['logical_file_name'], list) and len(data['logical_file_name'])>max_array_size):
                    dbsExceptionHandler("dbsException-invalid-input", 
                                        "The Max list length supported in listFileArray is %s." %max_array_size, self.logger.exception)
            #   
                ret =  self.dbsFile.listFiles(input_body=data)
        except cjson.DecodeError as De:
            dbsExceptionHandler('dbsException-invalid-input2', "Invalid input", self.logger.exception, str(De))
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except HTTPError as he:
            raise he
        except Exception as ex:
            sError = "DBSReaderModel/listFileArray. %s \n Exception trace: \n %s" \
            % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', ex.message, self.logger.exception, sError)
        for item in ret:
            yield item

    @transformInputType('run_num')
    @inputChecks(block_name=basestring, dataset=basestring, run_num=(long, int, basestring, list), validFileOnly=(int, basestring), sumOverLumi=(int, basestring))
    @thr.make_throttled()
    def listFileSummaries(self, block_name='', dataset='', run_num=-1, validFileOnly=0, sumOverLumi=0):
        """
        API to list number of files, event counts and number of lumis in a given block or dataset. 
        If the optional run_num, output are:

                * The number of files which have data (lumis) for that run number;
                * The total number of events in those files;
                * The total number of lumis for that run_number. Note that in general this is different from the total 
                number of lumis in those files, since lumis are filtered by the run_number they belong to, while events 
                are only counted as total per file in the data before  run 3. Howvere, when sumOverLumi=1, events will count by lumi when run_num
                is given while event_count/lumi is filled. If sumOverLumi=1, but event_count/lumi is not filled for any of the lumis in the block or
                dataset, then the API will return NULL for num_event. 
                * The total num blocks that have the run_num;
        Either block_name or dataset name is required. No wild-cards are allowed

        :param block_name: Block name
        :type block_name: str
        :param dataset: Dataset name
        :type dataset: str
        :param run_num: Run number (Optional). Possible format are: run_num, 'run_min-run_max' or ['run_min-run_max', run1, run2, ...]. run_num=1 is for MC data and caused almost full table scan. So run_num=1 will cause an input error.  
        :type run_num: int, str, list
        :param validFileOnly: default = 0. when = 1, only dataset_access_type = valid or production and is_file_valid=1 counted.
        :type validFileOnly: int
        :param sumOverLumi: default = 0. when = 1 count event_num by event_count/lumi.
        :type sumOverLumi: int  
        :returns: List of dictionaries containing the following keys (num_files, num_lumi, num_block, num_event, file_size)
        :rtype: list of dicts

        """
        # run_num=1 caused full table scan and CERN DBS reported some of the queries ran more than 50 hours
        # We will disbale all the run_num=1 calls in DBS.
        # YG Jan. 16 2019
        #
        if (run_num != -1)  :
            for r in parseRunRange(run_num):
                if isinstance(r, basestring) or isinstance(r, int) or isinstance(r, long):    
                    if r == 1 or r == '1':
                        dbsExceptionHandler("dbsException-invalid-input", "Run_num=1 is not a valid input.",
                                self.logger.exception)
                elif isinstance(r, run_tuple):
                    if r[0] == r[1]:
                        dbsExceptionHandler('dbsException-invalid-input', "DBS run range must be apart at least by 1.",
                          self.logger.exception)
                    elif r[0] <= 1 <= r[1]:
                        dbsExceptionHandler("dbsException-invalid-input", "Run_num=1 is not a valid input.",
                                self.logger.exception)
        try:
            r = self.dbsFile.listFileSummary(block_name, dataset, run_num, validFileOnly=validFileOnly, sumOverLumi=sumOverLumi)
	    for item in r:
		yield item	
	except HTTPError as he:
	    raise he
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
	except HTTPerror as he:
	    raise he
        except Exception as ex:
            sError = "DBSReaderModel/listFileSummaries. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', ex.message, self.logger.exception, sError)

    @inputChecks(dataset=basestring)
    @thr.make_throttled()
    def listDatasetParents(self, dataset=''):
        """
        API to list A datasets parents in DBS.

        :param dataset: dataset (Required)
        :type dataset: str
        :returns: List of dictionaries containing the following keys (this_dataset, parent_dataset_id, parent_dataset)
        :rtype: list of dicts

        """
        try:
            return self.dbsDataset.listDatasetParents(dataset)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception as ex:
            sError = "DBSReaderModel/listDatasetParents. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(dataset=basestring)
    @thr.make_throttled()
    def listDatasetChildren(self, dataset):
        """
        API to list A datasets children in DBS.

        :param dataset: dataset (Required)
        :type dataset: str
        :returns: List of dictionaries containing the following keys (child_dataset_id, child_dataset, dataset)
        :rtype: list of dicts

        """
        try:
            return self.dbsDataset.listDatasetChildren(dataset)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception as ex:
            sError = "DBSReaderModel/listDatasetChildren. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(dataset=basestring, logical_file_name=basestring, release_version=basestring, pset_hash=basestring, app_name=basestring,\
                 output_module_label=basestring, block_id=(int, basestring), global_tag=basestring)
    @thr.make_throttled()
    def listOutputConfigs(self, dataset="", logical_file_name="",
                          release_version="", pset_hash="", app_name="",
                          output_module_label="", block_id=0, global_tag=''):
        """
        API to list OutputConfigs in DBS.

        * You can use any combination of these parameters in this API
        * All parameters are optional, if you do not provide any parameter, all configs will be listed from DBS

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

        """
        release_version = release_version.replace("*", "%")
        pset_hash = pset_hash.replace("*", "%")
        app_name = app_name.replace("*", "%")
        output_module_label = output_module_label.replace("*", "%")
        try:
            return self.dbsOutputConfig.listOutputConfigs(dataset,
                logical_file_name, release_version, pset_hash, app_name,
                output_module_label, block_id, global_tag)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception as ex:
            sError = "DBSReaderModel/listOutputConfigs. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @transformInputType('logical_file_name')
    @inputChecks(logical_file_name=(basestring, list), block_id=(int, basestring), block_name=basestring)
    @thr.make_throttled()
    def listFileParents(self, logical_file_name='', block_id=0, block_name=''):
        """
        API to list file parents

        :param logical_file_name: logical_file_name of file (Required)
        :type logical_file_name: str, list
        :param block_id: ID of the a block, whose files should be listed
        :type block_id: int, str
        :param block_name: Name of the block, whose files should be listed
        :type block_name: int, str
        :returns: List of dictionaries containing the following keys (parent_logical_file_name, logical_file_name)
        :rtype: list of dicts

        """
        try:
            r = self.dbsFile.listFileParents(logical_file_name, block_id, block_name)
	    for item in r:
		yield item	
        except HTTPError as he:
	    raise he
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception as ex:
            sError = "DBSReaderModel/listFileParents. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', ex.message,  self.logger.exception, sError)
   
    @inputChecks(dataset=basestring)
    @thr.make_throttled()
    def listParentDSTrio(self, dataset=''):
        """
        API to list the parent block's trios. This is a API for WMAgent only.

        :param dataset: Name of the child dataset, whose parent dataset's trio (run, lumi and file) should be listed.
        :type dataset: str
        :returns: List of dicts containing the following key-value pairs:{file_id: [[run_num,  lumi] ... [run_num_n, lumin_n]]}
        :rtype: list of dicts

        """
        if not dataset:
            dbsExceptionHandler('dbsException-invalid-input', \
                "Child dataset is required for DBSReaderModel/listParentDSTrio", self.logger.exception )
        try:
            #import time
            #t1=time.time()
            with self.dbi.connection() as conn:
                r  = self.ParentDSFileLumiIdsDAO.execute(conn, dataset)
                for item in r:
                    yield item
            #print("Server time is: %.4f" %(time.time()-t1))
        except HTTPError as he:
            raise he
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception as ex:
            sError = "DBSReaderModel/listParentDSTrio. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', ex.message,  self.logger.exception, sError)

    @inputChecks(block_name=basestring)
    @thr.make_throttled()
    def listBlockTrio(self, block_name=''):
        """
        IMPORTANT: This is ***WMAgent*** sepcial case API. It is not for others.

        API to list the trios (run, lumi, file) for a given block with or w/o a list of LFN. 
        
        :param block_name: the block name 
        :type  block_name: str
        :returns: List of dicts containing the following key-value pairs:{file_id: [[run_num,  lumi] ... [run_num_n, lumin_n]]}
        :rtype: list of dicts
        
        """
        if not block_name:
            dbsExceptionHandler('dbsException-invalid-input', \
                "block_name is required for DBSReaderModel/listBlockTrio", self.logger.exception )
        try:
            with self.dbi.connection() as conn:
                r  = self.BKFileLumiIdsDAO.execute(conn, block_name)
                for item in r:
                    yield item
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except HTTPError as he:
            raise he
        except Exception as ex:
            sError = "DBSReaderModel/listBlockTrio. %s \n Exception trace: \n %s" \
            % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', ex.message, self.logger.exception, sError)

 
    @thr.make_throttled()
    def listFileParentsByLumi(self):
        """
        IMPORTANT: This is ***WMAgent*** sepcial case API. It is not for others.

        API to list File Parentage  for a given block with or w/o a list of LFN. It is used with the POST method of fileparents call.
        Using the child_lfn_list will significantly affect the API running speed. 
        
        :param block_name: This this the child's block name 
        :type  block_name: str
        :param logical_file_name: is a list of child lfn.  Max length 1000.  
        :type  logial_file_name: list of str
        :returns: List of dictionaries containing the following keys:[{child_parent_id_list: [(cid1, pid1), (cid2, pid2), ... (cidn, pidn)]}]
        :rtype: list of dicts
        
        """
        try :
            body = request.body.read()
            if body:
                data = cjson.decode(body)
                data = validateJSONInputNoCopy('file_parent_lumi', data, read=True)
            else:
                data = {}

            #Because CMSWEB has a 300 seconds responding time. We have to limit the array siz to make sure that
            #the API can be finished in 300 second. 
            max_array_size = 1000
            if ('logical_file_name' in data.keys() and isinstance(data['logical_file_name'], list) and len(data['logical_file_name'])>max_array_size):
                dbsExceptionHandler("dbsException-invalid-input",
                                        "The Max list length supported in listFilePArentsByLumi is %s." %max_array_size, self.logger.exception)

            lfn = []
            if "block_name" not in data.keys():
                 dbsExceptionHandler('dbsException-invalid-input', "block_name is required for fileparentsbylumi")
            else:
                if "logical_file_name" in data.keys():
                    lfn = data["logical_file_name"]
            result = self.dbsFile.listFileParentsByLumi(block_name=data["block_name"], logical_file_name=lfn)
            for r in result:
                yield r
        except cjson.DecodeError as De:
            dbsExceptionHandler('dbsException-invalid-input2', "Invalid input", self.logger.exception, str(De))
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except HTTPError as he:
            raise he
        except Exception as ex:
            sError = "DBSReaderModel/listFileParentsByLumi. %s \n Exception trace: \n %s" \
            % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', ex.message, self.logger.exception, sError)

    @transformInputType('logical_file_name')
    @inputChecks(logical_file_name=(basestring, list), block_name=(basestring), block_id=(basestring, int))
    @thr.make_throttled()
    def listFileChildren(self, logical_file_name='', block_name='', block_id=0):
        """
        API to list file children. One of the parameters in mandatory.

        :param logical_file_name: logical_file_name of file (Required)
        :type logical_file_name: str, list
        :param block_name: block_name
        :type block_name: str
        :param block_id: block_id
        :type block_id: str, int
        :returns: List of dictionaries containing the following keys (child_logical_file_name, logical_file_name)
        :rtype: List of dicts

        """
        if isinstance(logical_file_name, list):
            for f in logical_file_name:
                if '*' in f or '%' in f:
                    dbsExceptionHandler("dbsException-invalid-input2", dbsExceptionCode["dbsException-invalid-input2"], self.logger.exception, "No \
                                         wildcard allow in LFN list" )

        try:
            return self.dbsFile.listFileChildren(logical_file_name, block_name, block_id)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception as ex:
            sError = "DBSReaderModel/listFileChildren. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @transformInputType('run_num')
    @inputChecks(logical_file_name=(basestring, list), block_name=basestring, run_num=(long, int, basestring, list), validFileOnly=(int, basestring))
    @thr.make_throttled()
    def listFileLumis(self, logical_file_name="", block_name="", run_num=-1, validFileOnly=0):
        """
        API to list Lumi for files. Either logical_file_name or block_name is required. No wild card support in this API

        :param block_name: Name of the block
        :type block_name: str
        :param logical_file_name: logical_file_name of file
        :type logical_file_name: str, list
        :param run_num: List lumi sections for a given run number (Optional). Possible format are: run_num, 'run_min-run_max' or ['run_min-run_max', run1, run2, ...]. run_num=1 is for MC data and caused almost full table scan. So run_num=1
                        will cause an input error.
        :type run_num: int, str, or list
        :returns: List of dictionaries containing the following keys (lumi_section_num, logical_file_name, run_num, event_count)
        :rtype: list of dicts
        :param validFileOnly: optional valid file flag. Default = 0 (include all files)
        :type: validFileOnly: int, or str

        """
        # run_num=1 caused full table scan and CERN DBS reported some of the queries ran more than 50 hours
        # We will disbale all the run_num=1 calls in DBS. Run_num=1 will be OK when logical_file_name is given.
        # YG Jan. 16 2019
        if (run_num != -1  and logical_file_name ==''):
            for r in parseRunRange(run_num):
                if isinstance(r, basestring) or isinstance(r, int) or isinstance(r, long):    
                    if r == 1 or r == '1':
                        dbsExceptionHandler("dbsException-invalid-input", "Run_num=1 is not a valid input.",
                                self.logger.exception)
                elif isinstance(r, run_tuple):
                    if r[0] == r[1]:
                        dbsExceptionHandler("dbsException-invalid-input", "DBS run range must be apart at least by 1.",self.logger.exception)
                    elif r[0] <= 1 <= r[1]:
                        dbsExceptionHandler("dbsException-invalid-input", "Run_num=1 is not a valid input.",
                                self.logger.exception) 
        try:
            return self.dbsFile.listFileLumis(logical_file_name, block_name, run_num, validFileOnly )
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception as ex:
            sError = "DBSReaderModel/listFileLumis. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @thr.make_throttled()
    def listFileLumiArray(self):
        """
	API to list FileLumis for a given list of LFN. It is used with the POST method of fileLumis call.
	
        :param logical_file_name. Max length 1000.  
	:type logical_file_name: list of str
        :param block_name
        :type block_name: str
       	:param run_num  Possible format are: run_num, 'run_min-run_max' or ['run_min-run_max', run1, run2, ...].
	:type list, str or int 
	:param validFileOnly
	:type str or int
	:returns: List of dictionaries containing the following keys (lumi_section_num, logical_file_name, run_num)
	:rtype: list of dicts
	
	"""
	try :
	    body = request.body.read()
	    if body:
		data = cjson.decode(body)
		data = validateJSONInputNoCopy("files", data, read=True)
            else:
                data = {}

            #Because CMSWEB has a 300 seconds responding time. We have to limit the array siz to make sure that
            #the API can be finished in 300 second. 
            # YG Nov-05-2015
            max_array_size = 1000 
            if ( 'run_num' in data.keys() and isinstance(data['run_num'], list) and len(data['run_num'])>max_array_size)\
            or ('logical_file_name' in data.keys() and isinstance(data['logical_file_name'], list) and len(data['logical_file_name'])>max_array_size):
                dbsExceptionHandler("dbsException-invalid-input",
                                        "The Max list length supported in listLumiArray is %s." %max_array_size, self.logger.exception)
 
            if 'run_num' in data.keys() and not isinstance(data['run_num'], list) and (data['run_num']==1 or data['run_num']=="1"):
                dbsExceptionHandler('dbsException-invalid-input', 'run_num cannot be 1 in filelumiarray API.',
			self.logger.exception)
            if 'run_num' in data.keys() and isinstance(data['run_num'], list) and (1 in data['run_num'] or "1" in data['run_num']):
                dbsExceptionHandler('dbsException-invalid-input', 'run_num cannot be 1 in filelumiarray API.', 
			self.logger.exception)
	    result = self.dbsFile.listFileLumis(input_body=data)
	    for r in result:
	    	yield r
	except cjson.DecodeError as De:
	    dbsExceptionHandler('dbsException-invalid-input2', "Invalid input", self.logger.exception, str(De))
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
	except HTTPError as he:
	    raise he
	except Exception as ex:
	    sError = "DBSReaderModel/listFileLumiArray. %s \n Exception trace: \n %s" \
            % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', ex.message, self.logger.exception, sError)


    @transformInputType('run_num')
    @inputChecks(run_num=(long, int, basestring, list), logical_file_name=basestring, block_name=basestring, dataset=basestring)
    @thr.make_throttled()
    def listRuns(self, run_num=-1, logical_file_name="", block_name="", dataset=""):
        """
        API to list all runs in DBS. At least one parameter is mandatory.

        :param logical_file_name: List all runs in the file
        :type logical_file_name: str
        :param block_name: List all runs in the block
        :type block_name: str
        :param dataset: List all runs in that dataset
        :type dataset: str
        :param run_num: List all runs
        :type run_num: int, string or list

        """
        # run_num=1 caused full table scan and CERN DBS reported some of the queries ran more than 50 hours
        # We will disbale all the run_num=1 calls in DBS. Run_num=1 will be OK when logical_file_name is given.
        # YG Jan. 16 2019
        if (run_num != -1  and logical_file_name ==''):
            for r in parseRunRange(run_num):
                if isinstance(r, basestring) or isinstance(r, int) or isinstance(r, long):    
                    if r == 1 or r == '1':
                        dbsExceptionHandler("dbsException-invalid-input", "Run_num=1 is not a valid input.",
                                self.logger.exception)
                elif isinstance(r, run_tuple):
                    if r[0] == r[1]:
                        dbsExceptionHandler("dbsException-invalid-input", "DBS run range must be apart at least by 1.",
                          self.logger.exception)
                    elif r[0] <= 1 <= r[1]:
                        dbsExceptionHandler("dbsException-invalid-input", "Run_num=1 is not a valid input.",
                                self.logger.exception)
        if run_num==-1 and not logical_file_name and not dataset and not block_name:
                dbsExceptionHandler("dbsException-invalid-input",
                                    "run_num, logical_file_name, block_name or dataset parameter is mandatory",
                                    self.logger.exception)
        try:
            if logical_file_name:
                logical_file_name = logical_file_name.replace("*", "%")
            if block_name:
                block_name = block_name.replace("*", "%")
            if dataset:
                dataset = dataset.replace("*", "%")
            return self.dbsRun.listRuns(run_num, logical_file_name, block_name, dataset)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception as ex:
            sError = "DBSReaderModel/listRun. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(datatype=basestring, dataset=basestring)
    @thr.make_throttled()
    def listDataTypes(self, datatype="", dataset=""):
        """
        API to list data types known to dbs (when no parameter supplied).

        :param dataset: Returns data type (of primary dataset) of the dataset (Optional)
        :type dataset: str
        :param datatype: List specific data type
        :type datatype: str
        :returns: List of dictionaries containing the following keys (primary_ds_type_id, data_type)
        :rtype: list of dicts

        """
        try:
            return  self.dbsDataType.listDataType(dataType=datatype, dataset=dataset)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception as ex:
            sError = "DBSReaderModel/listDataTypes. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(block_name=basestring)
    def dumpBlock(self, block_name):
        """
        API the list all information related with the block_name

        :param block_name: Name of block to be dumped (Required)
        :type block_name: str

        """
        try:
            return self.dbsBlock.dumpBlock(block_name)
        except HTTPError as he:
            raise he
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception as ex:
            sError = "DBSReaderModel/dumpBlock. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', ex.message, self.logger.exception, sError)

    @inputChecks(acquisition_era_name=basestring)
    @thr.make_throttled()
    def listAcquisitionEras(self, acquisition_era_name=''):
        """
        API to list all Acquisition Eras in DBS.

        :param acquisition_era_name: Acquisition era name (Optional, wild cards allowed)
        :type acquisition_era_name: str
        :returns: List of dictionaries containing following keys (description, end_date, acquisition_era_name, create_by, creation_date and start_date)
        :rtype: list of dicts

        """
        try:
            acquisition_era_name = acquisition_era_name.replace('*', '%')
            return  self.dbsAcqEra.listAcquisitionEras(acquisition_era_name)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception as ex:
            sError = "DBSReaderModel/listAcquisitionEras. %s\n. Exception trace: \n %s" % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(acquisition_era_name=basestring)
    @thr.make_throttled()
    def listAcquisitionEras_CI(self, acquisition_era_name=''):
        """
        API to list ALL Acquisition Eras (case insensitive) in DBS.

        :param acquisition_era_name: Acquisition era name (Optional, wild cards allowed)
        :type acquisition_era_name: str
        :returns: List of dictionaries containing following keys (description, end_date, acquisition_era_name, create_by, creation_date and start_date)
        :rtype: list of dicts

        """
        try:
            acquisition_era_name = acquisition_era_name.replace('*', '%')
            return  self.dbsAcqEra.listAcquisitionEras_CI(acquisition_era_name)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception as ex:
            sError = "DBSReaderModel/listAcquisitionEras. %s\n. Exception trace: \n %s" % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'],
                                self.logger.exception, sError)

    @inputChecks(processing_version=(basestring, int))
    @thr.make_throttled()
    def listProcessingEras(self, processing_version=0):
        """
        API to list all Processing Eras in DBS.

        :param processing_version: Processing Version (Optional). If provided just this processing_version will be listed
        :type processing_version: str
        :returns: List of dictionaries containing the following keys (create_by, processing_version, description, creation_date)
        :rtype: list of dicts

        """
        try:
            #processing_version = processing_version.replace("*", "%")
            return  self.dbsProcEra.listProcessingEras(processing_version)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception as ex:
            sError = "DBSReaderModel/listProcessingEras. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(release_version=basestring, dataset=basestring, logical_file_name=basestring)
    @thr.make_throttled()
    def listReleaseVersions(self, release_version='', dataset='', logical_file_name=''):
        """
        API to list all release versions in DBS

        :param release_version: List only that release version
        :type release_version: str
        :param dataset: List release version of the specified dataset
        :type dataset: str
        :param logical_file_name: List release version of the logical file name
        :type logical_file_name: str
        :returns: List of dictionaries containing following keys (release_version)
        :rtype: list of dicts

        """
        if release_version:
            release_version = release_version.replace("*", "%")
        try:
            return self.dbsReleaseVersion.listReleaseVersions(release_version, dataset, logical_file_name )
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception as ex:
            sError = "DBSReaderModel/listReleaseVersions. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(dataset_access_type=basestring)
    @thr.make_throttled()
    def listDatasetAccessTypes(self, dataset_access_type=''):
        """
        API to list dataset access types.

        :param dataset_access_type: List that dataset access type (Optional)
        :type dataset_access_type: str
        :returns: List of dictionary containing the following key (dataset_access_type).
        :rtype: List of dicts

        """
        if dataset_access_type:
            dataset_access_type = dataset_access_type.replace("*", "%")
        try:
            return self.dbsDatasetAccessType.listDatasetAccessTypes(dataset_access_type)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception as ex:
            sError = "DBSReaderModel/listDatasetAccessTypes. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(physics_group_name=basestring)
    @thr.make_throttled()
    def listPhysicsGroups(self, physics_group_name=''):
        """
        API to list all physics groups.

        :param physics_group_name: List that specific physics group (Optional)
        :type physics_group_name: basestring
        :returns: List of dictionaries containing the following key (physics_group_name)
        :rtype: list of dicts

        """
        if physics_group_name:
            physics_group_name = physics_group_name.replace('*', '%')
        try:
            return self.dbsPhysicsGroup.listPhysicsGroups(physics_group_name)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception as ex:
            sError = "DBSReaderModel/listPhysicsGroups. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(dataset=basestring, run_num=(basestring, int, long))
    @thr.make_throttled()
    def listRunSummaries(self, dataset="", run_num=-1):
        """
        API to list run summaries, like the maximal lumisection in a run.

        :param dataset: dataset name (Optional)
        :type dataset: str
        :param run_num: Run number (Required)
        :type run_num: str, long, int
        :rtype: list containing a dictionary with key max_lumi
        """
        if run_num==-1:
            dbsExceptionHandler("dbsException-invalid-input",
                                "The run_num parameter is mandatory",
                                self.logger.exception)

        if re.search('[*,%]', dataset):
            dbsExceptionHandler("dbsException-invalid-input",
                                "No wildcards are allowed in dataset",
                                self.logger.exception)
        # run_num=1 caused full table scan and CERN DBS reported some of the queries ran more than 50 hours
        # We will disbale all the run_num=1 calls in DBS. Run_num=1 will be OK when dataset is given in this API.
        # YG Jan. 16 2019
        if ((run_num == -1 or run_num == '-1') and dataset==''):
            dbsExceptionHandler("dbsException-invalid-input", "Run_num=1 is not a valid input when no dataset is present.", 
                                 self.logger.exception)
        conn = None
        try:
            conn = self.dbi.connection()
            return self.dbsRunSummaryListDAO.execute(conn, dataset, run_num)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception as ex:
            sError = "DBSReaderModel/listRunSummaries. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'],
                                self.logger.exception, sError)
        finally:
            if conn:
                conn.close()
