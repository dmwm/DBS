#!/usr/bin/env python
#pylint: disable=C0103
"""
DBS Reader Rest Model module
"""

__revision__ = "$Id: DBSReaderModel.py,v 1.50 2010/08/13 20:38:37 yuyi Exp $"
__version__ = "$Revision: 1.50 $"

import cjson
import inspect
import traceback

from cherrypy.lib import profiler
import cProfile
from cherrypy import request, tools, HTTPError

from WMCore.WebTools.RESTModel import RESTModel

from dbs.utils.dbsUtils import dbsUtils
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
from dbs.business.DBSStatus import DBSStatus
from dbs.business.DBSBlockInsert import DBSBlockInsert
from dbs.business.DBSReleaseVersion import DBSReleaseVersion
from dbs.business.DBSDatasetAccessType import DBSDatasetAccessType
from dbs.business.DBSPhysicsGroup import DBSPhysicsGroup
from dbs.utils.dbsException import dbsException, dbsExceptionCode
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.DBSInputValidation import *
from dbs.utils.DBSTransformInputType import transformInputType
from WMCore.DAOFactory import DAOFactory

__server__version__ = "$Name:  $"

#Necessary for sphinx documentation and server side unit tests.
if not getattr(tools,"secmodv2",None):
    class FakeAuthForDoc(object):
        def __init__(self,*args,**kwargs):
            pass

        def callable(self, role=[], group=[], site=[], authzfunc=None):
            pass

    tools.secmodv2 = FakeAuthForDoc()

class DBSReaderModel(RESTModel):
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
        if type(urls)==type({}):
            config.database.connectUrl = urls['reader']

        dbowner = config.database.dbowner

        RESTModel.__init__(self, config)
        self.dbsUtils2 = dbsUtils()
        self.version = self.getServerVersion()
        #self.warning("DBSReaderModle")
        #self.logger.warning("DBSReaderModle")
        self.methods = {'GET':{}, 'PUT':{}, 'POST':{}, 'DELETE':{}}

        self.daofactory = DAOFactory(package='dbs.dao', logger=self.logger, dbinterface=self.dbi, owner=dbowner)

        self.dbsDataTierListDAO = self.daofactory(classname="DataTier.List")

	self._addMethod('GET', 'serverinfo', self.getServerInfo)
        #self._addMethod('GET', 'donothing', self.donothing)
        self._addMethod('GET', 'primarydatasets', self.listPrimaryDatasets, args=['primary_ds_name', 'primary_ds_type'])
        self._addMethod('GET', 'primarydstypes', self.listPrimaryDsTypes, args=['primary_ds_type', 'dataset'])
        self._addMethod('GET', 'datasets', self.listDatasets, args=['dataset', 'parent_dataset', 'release_version',
                                'pset_hash', 'app_name', 'output_module_label', 'processing_version',
                                'acquisition_era_name', 'run_num','physics_group_name', 'logical_file_name',
                                'primary_ds_name', 'primary_ds_type', 'processed_ds_name', 'data_tier_name',
                                'dataset_access_type', 'prep_id', 'create_by', 'last_modified_by',
                                'min_cdate', 'max_cdate', 'min_ldate', 'max_ldate', 'cdate', 'ldate', 'detail'])
        self._addMethod('POST', 'datasetlist', self.listDatasetArray)
        self._addMethod('GET', 'blocks', self.listBlocks, args=['dataset', 'block_name', 'origin_site_name',
                        'logical_file_name', 'run_num', 'min_cdate', 'max_cdate', 'min_ldate',
                        'max_ldate', 'cdate', 'ldate', 'detail'])
        self._addMethod('GET', 'blockorigin', self.listBlockOrigin, args=['origin_site_name', 'dataset'])
        self._addMethod('GET', 'files', self.listFiles, args=['dataset', 'block_name', 'logical_file_name',
                        'release_version', 'pset_hash', 'app_name', 'output_module_label', 'minrun', 'maxrun',
                        'origin_site_name', 'lumi_list', 'detail'])
        self._addMethod('GET', 'filesummaries', self.listFileSummaries, args=['block_name', 'dataset',
                        'run_num'])
        self._addMethod('GET', 'datasetparents', self.listDatasetParents, args=['dataset'])
        self._addMethod('GET', 'datasetchildren', self.listDatasetChildren, args=['dataset'])
        self._addMethod('GET', 'outputconfigs', self.listOutputConfigs, args=['dataset', 'logical_file_name',
                        'release_version', 'pset_hash', 'app_name', 'output_module_label', 'block_id', 'global_tag'])
        self._addMethod('GET', 'fileparents', self.listFileParents, args=['logical_file_name', 'block_id',
                        'block_name'])
        self._addMethod('GET', 'filechildren', self.listFileChildren, args=['logical_file_names', 'block_name', 'block_id'])
        self._addMethod('GET', 'filelumis', self.listFileLumis, args=['logical_file_name', 'block_name', 'run_num'])
        self._addMethod('GET', 'runs', self.listRuns, args=['minrun', 'maxrun', 'logical_file_name',
                        'block_name', 'dataset'])
        self._addMethod('GET', 'datatypes', self.listDataTypes, args=['datatype', 'dataset'])
        self._addMethod('GET','datatiers',self.listDataTiers, args=['data_tier_name'])
        self._addMethod('GET', 'blockparents', self.listBlockParents, args=['block_name'])
        self._addMethod('POST', 'blockparents', self.listBlocksParents)
        self._addMethod('GET', 'blockchildren', self.listBlockChildren, args=['block_name'])
        self._addMethod('GET', 'blockdump', self.dumpBlock, args=['block_name'])
        self._addMethod('GET', 'acquisitioneras', self.listAcquisitionEras, args=['acquisition_era_name'])
        self._addMethod('GET', 'acquisitioneras_ci', self.listAcquisitionEras_CI, args=['acquisition_era_name'])
        self._addMethod('GET', 'processingeras', self.listProcessingEras, args=['processing_version'])
        self._addMethod('GET', 'releaseversions', self.listReleaseVersions, args=['release_version', 'dataset', 'logical_file_name'])
        self._addMethod('GET', 'datasetaccesstypes', self.listDatasetAccessTypes, args=['dataset_access_type'])
        self._addMethod('GET', 'physicsgroups', self.listPhysicsGroups, args=['physics_group_name'])
        self._addMethod('GET', 'help', self.getHelp, args=['call'])

        self.dbsDoNothing = DBSDoNothing(self.logger, self.dbi, dbowner)
        self.dbsPrimaryDataset = DBSPrimaryDataset(self.logger, self.dbi, dbowner)
        self.dbsDataset = DBSDataset(self.logger, self.dbi, dbowner)
        self.dbsBlock = DBSBlock(self.logger, self.dbi, dbowner)
        self.dbsFile = DBSFile(self.logger, self.dbi, dbowner)
        self.dbsAcqEra = DBSAcquisitionEra(self.logger, self.dbi,
            dbowner)
        self.dbsOutputConfig = DBSOutputConfig(self.logger, self.dbi,
            dbowner)
        self.dbsProcEra = DBSProcessingEra(self.logger, self.dbi,
            dbowner)
        self.dbsSite = DBSSite(self.logger, self.dbi, dbowner)
	self.dbsRun = DBSRun(self.logger, self.dbi, dbowner)
	self.dbsDataType = DBSDataType(self.logger, self.dbi, dbowner)
        self.dbsStatus = DBSStatus(self.logger, self.dbi, dbowner)
        self.dbsBlockInsert = DBSBlockInsert(self.logger, self.dbi, dbowner)
        self.dbsReleaseVersion = DBSReleaseVersion(self.logger, self.dbi, dbowner)
        self.dbsDatasetAccessType = DBSDatasetAccessType(self.logger, self.dbi, dbowner)
        self.dbsPhysicsGroup = DBSPhysicsGroup(self.logger, self.dbi, dbowner)
    """
    def checkList(self, input):
        if type(input['block_name']) is not str:
                raise Val....
        return input
    """
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
            params = self.methods['GET'][call]['args']
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

    """
    Used for Stress test.
    def donothing(self):
        return self.dbsDoNothing.listNone()
    """

    @inputChecks(primary_ds_name=str, primary_ds_type=str)
    def listPrimaryDatasets(self, primary_ds_name="", primary_ds_type=""):
        """
        API to list ALL primary datasets in DBS

        :param primary_ds_name: If provided, will list that primary dataset
        :type primary_ds_name: str
        :param primary_ds_type:  If provided, will list all primary dataset having that type
        :type primary_ds_name: str
        :returns: List of dictionaries containing the following keys (create_by, primary_ds_type, primary_ds_id, primary_ds_name, creation_date)
        :rtype: list of dicts

        """
        primary_ds_name = primary_ds_name.replace("*","%")
        primary_ds_type = primary_ds_type.replace("*","%")
        try:
            return self.dbsPrimaryDataset.listPrimaryDatasets(primary_ds_name, primary_ds_type)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except Exception, ex:
            sError = "DBSReaderModel/listPrimaryDatasets. %s\n Exception trace: \n %s." \
                    % (ex, traceback.format_exc() )
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(primary_ds_type=str, dataset=str)
    def listPrimaryDsTypes(self, primary_ds_type="", dataset=""):
        """
        API to list primary dataset types

        :param primary_ds_type: If provided, will list that primary dataset type
        :type primary_ds_type: str
        :param dataset: List the primary dataset type for that dataset
        :type dataset: str
        :returns: List of dictionaries containing the following keys (primary_ds_type_id, data_type)
        :rtype: list of dicts

        """
        if primary_ds_type:
            primary_ds_type = primary_ds_type.replace("*","%")
        if dataset:
            dataset = dataset.replace("*","%")
        try:
            return self.dbsPrimaryDataset.listPrimaryDSTypes(primary_ds_type, dataset)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except Exception, ex:
            sError = "DBSReaderModel/listPrimaryDsTypes. %s\n. Exception trace: \n %s" \
                % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    #@expose
    @inputChecks( dataset=str, parent_dataset=str, release_version=str, pset_hash=str,
                 app_name=str, output_module_label=str,  processing_version=(int,str), acquisition_era_name=str,
                 run_num=(long,int,str), physics_group_name=str, logical_file_name=str, primary_ds_name=str,
                 primary_ds_type=str, processed_ds_name=str, data_tier_name=str, dataset_access_type=str, prep_id=str,
                 create_by=(str), last_modified_by=(str), min_cdate=(int,str), max_cdate=(int,str),
                 min_ldate=(int,str), max_ldate=(int, str), cdate=(int,str), ldate=(int,str), detail=(bool,str))
    def listDatasets(self, dataset="", parent_dataset="", is_dataset_valid=1,
        release_version="", pset_hash="", app_name="", output_module_label="",
        processing_version=0, acquisition_era_name="", run_num="0",
        physics_group_name="", logical_file_name="", primary_ds_name="", primary_ds_type="",
        processed_ds_name='', data_tier_name="", dataset_access_type="VALID", prep_id='', create_by="", last_modified_by="",
        min_cdate='0', max_cdate='0', min_ldate='0', max_ldate='0', cdate='0',
        ldate='0', detail=False):
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
        processed_ds_name = processed_ds_name.replace("*", "%")
        acquisition_era_name = acquisition_era_name.replace("*", "%")
        #processing_version =  processing_version.replace("*", "%")
        #create_by and last_modified_by have be full spelled, no wildcard will allowed.
        #We got them from request head so they can be either HN account name or DN.
        #This is depended on how an user's account is set up.
        if create_by.find('*')!=-1 or create_by.find('%')!=-1 or last_modified_by.find('*')!=-1\
                or last_modified_by.find('%')!=-1:
            dbsExceptionHandler("dbsException-invalid-input", "Invalid Input for create_by or last_modified_by.\
            No wildcard allowed.",  self.logger.exception, 'No wildcards allowed for create_by or last_modified_by')
        try:
            #run_num = run_num.replace("*", "%")
            if isinstance(run_num,str) and ('*' in run_num or '%' in run_num):
                run_num = 0
            else:
                run_num = int(run_num)
            if isinstance(min_cdate,str) and ('*' in min_cdate or '%' in min_cdate):
                min_cdate = 0
            else:
                min_cdate = int(min_cdate)
            if isinstance(max_cdate,str) and ('*' in max_cdate or '%' in max_cdate):
                max_cdate = 0
            else:
                max_cdate = int(max_cdate)
            if isinstance(min_ldate,str) and ('*' in min_ldate or '%' in min_ldate):
                min_ldate = 0
            else:
                min_ldate = int(min_ldate)
            if isinstance(max_ldate,str) and ('*' in max_ldate or '%' in max_ldate):
                max_ldate = 0
            else:
                max_ldate = int(max_ldate)
            if isinstance(cdate,str) and ('*' in cdate or '%' in cdate):
                cdate = 0
            else:
                cdate = int(cdate)
            if isinstance(ldate,str) and ('*' in ldate or '%' in ldate):
                ldate = 0
            else:
                ldate = int(ldate)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception, ex:
            sError = "DBSReaderModel/listDatasets.  %s \n. Exception trace: \n %s" \
                % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

        detail = detail in (True, 1, "True", "1", 'true')
        try:
            return self.dbsDataset.listDatasets(dataset, parent_dataset, is_dataset_valid, release_version, pset_hash,
                app_name, output_module_label, processing_version, acquisition_era_name,
                run_num, physics_group_name, logical_file_name, primary_ds_name, primary_ds_type, processed_ds_name,
                data_tier_name, dataset_access_type, prep_id, create_by, last_modified_by,
                min_cdate, max_cdate, min_ldate, max_ldate, cdate, ldate, detail)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception, ex:
            sError = "DBSReaderModel/listdatasets. %s.\n Exception trace: \n %s" % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    def listDatasetArray(self):
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

        To be called by datasetlist url with post call.

        """
        try :
            body = request.body.read()
            if body:
                data = cjson.decode(body)
                #import pdb
                #pdb.set_trace()
                data = validateJSONInputNoCopy("dataset",data)
            else:
                data=''
            return self.dbsDataset.listDatasetArray(data)
        except cjson.DecodeError as De:
            dbsExceptionHandler('dbsException-invalid-input2', "Invalid input", self.logger.exception, str(De))
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except HTTPError as he:
            raise he
        except Exception, ex:
            sError = "DBSReaderModel/listDatasetArray. %s \n Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(data_tier_name=str)
    def listDataTiers(self, data_tier_name=""):
	"""
        API to list data tiers  known to DBS

        :param datatier: When supplied, dbs will list details on this tier (Optional)
        :type datatier: str
        :returns: List of dictionaries containing the following keys (data_tier_id, data_tier_name, create_by, creation_date)

	"""
	data_tier_name = data_tier_name.replace("*","%")

        try:
            conn = self.dbi.connection()
            return self.dbsDataTierListDAO.execute(conn,data_tier_name.upper())
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except ValueError as ve:
            dbsExceptionHandler("dbsException-invalid-input", "Invalid Input Data",  self.logger.exception, ve.message)
        except TypeError as te:
            dbsExceptionHandler("dbsException-invalid-input", "Invalid Input DataType",  self.logger.exception, te.message)
        except NameError as ne:
            dbsExceptionHandler("dbsException-invalid-input", "Invalid Input Searching Key",  self.logger.exception, ne.message)
        except Exception, ex:
            sError = "DBSReaderModel/listDataTiers. %s\n. Exception trace: \n %s" \
                    % ( ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)
        finally:
            if conn:
                conn.close()

    @inputChecks(dataset=str, block_name=str, origin_site_name=str, logical_file_name=str ,run_num=(long,int,str), min_cdate=(int,str), \
                 max_cdate=(int, str), min_ldate=(int,str), max_ldate=(int,str), cdate=(int,str),  ldate=(int,str), detail=(str,bool))
    def listBlocks(self, dataset="", block_name="", origin_site_name="",
        logical_file_name="",run_num='-1', min_cdate='0', max_cdate='0',
        min_ldate='0', max_ldate='0', cdate='0',  ldate='0', detail=False):
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
        dataset = dataset.replace("*","%")
        block_name = block_name.replace("*","%")
        logical_file_name = logical_file_name.replace("*","%")
        origin_site_name = origin_site_name.replace("*","%")
        run_num = str(run_num)
        try:
            if isinstance(run_num,str) and ('%' in run_num or '*' in run_num):
                run_num = 0
            else:
                run_num = int(run_num)
            if isinstance(min_cdate,str) and ('*' in min_cdate or '%' in min_cdate):
                min_cdate = 0
            else:
                min_cdate = int(min_cdate)
            if isinstance(max_cdate,str) and ('*' in max_cdate or '%' in max_cdate):
                max_cdate = 0
            else:
                max_cdate = int(max_cdate)
            if isinstance(min_ldate,str) and ('*' in min_ldate or '%' in min_ldate):
                min_ldate = 0
            else:
                min_ldate = int(min_ldate)
            if isinstance(max_ldate,str) and ('*' in max_ldate or '%' in max_ldate):
                max_ldate = 0
            else:
                max_ldate = int(max_ldate)
            if isinstance(cdate,str) and ('*' in cdate or '%' in cdate):
                cdate = 0
            else:
                cdate = int(cdate)
            if isinstance(cdate,str) and ('*' in ldate or '%' in ldate):
                ldate = 0
            else:
                ldate = int(ldate)
        except Exception, ex:
            sError = "DBSReaderModel/listBlocks.\n. %s \n Exception trace: \n %s" \
                                % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-invalid-input2',  str(ex), self.logger.exception, sError )
        detail = detail in (True, 1, "True", "1", 'true')
        try:
            return self.dbsBlock.listBlocks(dataset, block_name, origin_site_name, logical_file_name, run_num,
                min_cdate, max_cdate, min_ldate, max_ldate, cdate, ldate, detail)

        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception, ex:
            sError = "DBSReaderModel/listBlocks. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(origin_site_name=str, dataset=str)
    def listBlockOrigin(self, origin_site_name="",  dataset=""):
        """
        API to list blocks first generated in origin_site_name

        :param origin_site_name: Origin Site Name (Required, No wildcards)
        :type origin_site_name: str
        :param dataset: dataset (Required, No wildcards)
        :type dataset: str
        :returns: List of dictionaries containg the following keys (create_by, creation_date, open_for_writing, last_modified_by, dataset, block_name, file_count, origin_site_name, last_modification_date, block_size)
        :rtype: list of dicts

        """
        try:
            return self.dbsBlock.listBlocksOrigin(origin_site_name, dataset)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception, ex:
            sError = "DBSReaderModel/listBlocks. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)


    @inputChecks(block_name=str)
    def listBlockParents(self, block_name=""):
        """
        API to list block parents

        :param block_name: name of block whoes parents needs to be found (Required)
        :type block_name: str
        :returns: List of dictionaries containing following keys (block_name)
        :rtype: list of dicts

        """
        try:
            return self.dbsBlock.listBlockParents(block_name)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception, ex:
            sError = "DBSReaderModel/listBlockParents. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'],  self.logger.exception, sError)


    def listBlocksParents(self):
        """
        API to list block parents of multiple blocks

        :param block_names: list of block names [block_name1, block_name2, ...] (Required)
        :type block_names: list

        To be called by blockparents url with post call

        """
        try :
            body = request.body.read()
            data = cjson.decode(body)
            data = validateJSONInputNoCopy("block", data)
            return self.dbsBlock.listBlockParents(data["block_name"])
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except cjson.DecodeError, de:
            sError = "DBSReaderModel/listBlockParents. %s\n. Exception trace: \n %s" \
                    % (de, traceback.format_exc())
            msg = "DBSReaderModel/listBlockParents. %s" % de
            dbsExceptionHandler('dbsException-invalid-input2', msg, self.logger.exception, sError)
        except HTTPError as he:
            raise he
        except Exception, ex:
            sError = "DBSReaderModel/listBlockParents. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(block_name=str)
    def listBlockChildren(self, block_name=""):
        """
        API to list block children

        :param block_name: name of block whoes children needs to be found (Required)
        :type block_name: str
        :returns: List of dictionaries containing following keys (block_name)
        :rtype: list of dicts

        """
        block_name = block_name.replace("*","%")
        try:
            return self.dbsBlock.listBlockChildren(block_name)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception, ex:
            sError = "DBSReaderModel/listBlockChildren. %s\n. Exception trace: \n %s" % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(dataset =str, block_name=str, logical_file_name =str, release_version=str, pset_hash=str, app_name=str,\
                 output_module_label=str, minrun=(long, int, str), maxrun=(long, int,str), origin_site_name=str, lumi_list=(str,list), detail=(str,bool))
    def listFiles(self, dataset = "", block_name = "", logical_file_name = "",
        release_version="", pset_hash="", app_name="", output_module_label="",
        minrun=-1, maxrun=-1, origin_site_name="", lumi_list="", detail=False):
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
            lumi_list = self.dbsUtils2.decodeLumiIntervals(lumi_list)

        detail = detail in (True, 1, "True", "1", 'true')
        output_module_label = output_module_label.replace("*", "%")
        try:
            return self.dbsFile.listFiles(dataset, block_name, logical_file_name , release_version , pset_hash, app_name,
                                        output_module_label, maxrun, minrun, origin_site_name, lumi_list, detail)

        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception, ex:
            sError = "DBSReaderModel/listFiles. %s \n Exception trace: \n %s" % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'],
                    self.logger.exception, sError)

    @inputChecks(block_name=str, dataset=str, run_num=(long,int, str))
    def listFileSummaries(self, block_name='', dataset='', run_num=0):
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
        try:
            return self.dbsFile.listFileSummary(block_name, dataset, run_num)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception, ex:
            sError = "DBSReaderModel/listFileSummaries. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(dataset=str)
    def listDatasetParents(self, dataset=''):
        """
        API to list A datasets parents in DBS

        :param dataset: dataset (Required)
        :type dataset: str
        :returns: List of dictionaries containing the following keys (this_dataset, parent_dataset_id, parent_dataset)
        :rtype: list of dicts

        """
        try:
            return self.dbsDataset.listDatasetParents(dataset)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception, ex:
            sError = "DBSReaderModel/listDatasetParents. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(dataset=str)
    def listDatasetChildren(self, dataset):
        """
        API to list A datasets children in DBS

        :param dataset: dataset (Required)
        :type dataset: str
        :returns: List of dictionaries containing the following keys (child_dataset_id, child_dataset, dataset)
        :rtype: list of dicts

        """
        try:
            return self.dbsDataset.listDatasetChildren(dataset)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception, ex:
            sError = "DBSReaderModel/listDatasetChildren. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(dataset=str, logical_file_name=str, release_version=str, pset_hash=str, app_name=str,\
                 output_module_label=str, block_id=(int,str), global_tag=str)
    def listOutputConfigs(self, dataset="", logical_file_name="",
                          release_version="", pset_hash="", app_name="",
                          output_module_label="", block_id=0, global_tag=''):
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
        except Exception, ex:
            sError = "DBSReaderModel/listOutputConfigs. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(logical_file_name=str, block_id=(int,str), block_name=str)
    def listFileParents(self, logical_file_name='', block_id=0, block_name=''):
        """
        API to list file parents

        :param logical_file_name: logical_file_name of file (Required)
        :type logical_file_name: str
        :returns: List of dictionaries containing the following keys (parent_logical_file_name, logical_file_name)
        :rtype: list of dicts

        """
        try:
            return self.dbsFile.listFileParents(logical_file_name, block_id, block_name)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception, ex:
            sError = "DBSReaderModel/listFileParents. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @transformInputType('logical_file_names')
    @inputChecks(logical_file_names=(str, list), block_name=(str), block_id=(str, int))
    def listFileChildren(self, logical_file_names='', block_name='', block_id=0):
        """
        API to list file children

        :param logical_file_name: logical_file_name of file (Required)
        :type logical_file_name: str
        :returns: List of dictionaries containing the following keys (child_logical_file_name, logical_file_name)
        :rtype: List of dicts

        """
        if isinstance(logical_file_names, list):
            for f in logical_file_names:
                if '*' in f or '%' in f:
                    dbsExceptionHandler("dbsException-invalid-input2", dbsExceptionCode["dbsException-invalid-input2"],self.logger.exception,"No \
                                         wildcard allow in LFN list" )

        try:
            return self.dbsFile.listFileChildren(logical_file_names, block_name, block_id)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception, ex:
            sError = "DBSReaderModel/listFileChildren. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(logical_file_name=str, block_name=str, run_num=(long,int,str))
    def listFileLumis(self, logical_file_name="", block_name="", run_num='0'):
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
        if isinstance(run_num,str) and ('*' in run_num or '%' in run_num):
            run_num = 0
        else:
            run_num = int(run_num)
        try:
            return self.dbsFile.listFileLumis(logical_file_name, block_name, run_num )
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception, ex:
            sError = "DBSReaderModel/listFileLumis. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(minrun=(long, int,str), maxrun=(long, int,str), logical_file_name=str, block_name=str, dataset=str)
    def listRuns(self, minrun=-1, maxrun=-1, logical_file_name="",
                 block_name="", dataset=""):
        """
        API to list all runs in DBS. All parameters are optional.

        :param logical_file_name: List all runs in the file
        :type logical_file_name: str
        :param block_name: List all runs in the block
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
        try:
            if(logical_file_name):
                logical_file_name = logical_file_name.replace("*", "%")
                #print ("LFN=%s\n" %logical_file_name)
            if(block_name):
                block_name = block_name.replace("*", "%")
                #print("Block=%s\n" %block_name)
            if(dataset):
                dataset = dataset.replace("*", "%")
                #print("ds=%s\n" %dataset)
            #print ("maxrun=%s, minrun=%s\n" %(maxrun, minrun) )
            return self.dbsRun.listRuns(minrun, maxrun, logical_file_name, block_name, dataset)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception, ex:
            sError = "DBSReaderModel/listRun. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(datatype=str, dataset=str)
    def listDataTypes(self, datatype="", dataset=""):
        """
        API to list data types known to dbs (when no parameter supplied)

        :param dataset: Returns data type (of primary dataset) of the dataset (Optional)
        :type dataset: str
        :returns: List of dictionaries containing the following keys (primary_ds_type_id, data_type)
        :rtype: list of dicts

        """
        try:
            return  self.dbsDataType.listDataType(dataType=datatype, dataset=dataset)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception, ex:
            sError = "DBSReaderModel/listDataTypes. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(block_name=str)
    def dumpBlock(self, block_name):
        """
        API the list all information related with the block_name

        :param block_name: Name of block whoes children needs to be found (Required)
        :type block_name: str

        """
        try:
            return self.dbsBlock.dumpBlock(block_name)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception, ex:
            sError = "DBSReaderModel/dumpBlock. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(acquisition_era_name=str)
    def listAcquisitionEras(self, acquisition_era_name=''):
        """
        API to list ALL Acquisition Eras in DBS

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

    @inputChecks(acquisition_era_name=str)
    def listAcquisitionEras_CI(self, acquisition_era_name=''):
        """
        API to list ALL Acquisition Eras (case insensitive) in DBS

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

    @inputChecks(processing_version=(str,int))
    def listProcessingEras(self, processing_version=0):
        """
        API to list all Processing Eras in DBS

        :param processing_version: Processing Version (Optional). If provided just this processing_version will be listed
        :type processing_version: str
        :returns: List of dictionaries containg the following keys (create_by, processing_version, description, creation_date)
        :rtype: list of dicts

        """
        try:
            #processing_version = processing_version.replace("*", "%")
            return  self.dbsProcEra.listProcessingEras(processing_version)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception, ex:
            sError = "DBSReaderModel/listProcessingEras. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(release_version=str, dataset=str, logical_file_name=str)
    def listReleaseVersions(self, release_version='', dataset='', logical_file_name=''):
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
        if release_version:
            release_version = release_version.replace("*","%")
        try:
            return self.dbsReleaseVersion.listReleaseVersions(release_version, dataset, logical_file_name )
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception, ex:
            sError = "DBSReaderModel/listReleaseVersions. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(dataset_access_type=str)
    def listDatasetAccessTypes(self, dataset_access_type=''):
        """
        API to list ALL dataset access types

        :param dataset_access_type: If provided, list that dataset access type (Optional)
        :type dataset_access_type: str
        :returns: List of dictionary containg the following key (dataset_access_type).
        :rtype: List of dicts

        """
        if dataset_access_type:
            dataset_access_type = dataset_access_type.replace("*","%")
        try:
            return self.dbsDatasetAccessType.listDatasetAccessTypes(dataset_access_type)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception, ex:
            sError = "DBSReaderModel/listDatasetAccessTypes. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @inputChecks(physics_group_name=str)
    def listPhysicsGroups(self, physics_group_name=''):
        """
        API to list ALL physics groups

        :param physics_group_name: If provided, list that specific physics group
        :type physics_group_name: str
        :returns: List of dictionaries containing the following key (physics_group_name)
        :rtype: list of dicts

        """
        if physics_group_name:
            physics_group_name = physics_group_name.replace('*', '%')
        try:
            return self.dbsPhysicsGroup.listPhysicsGroups(physics_group_name)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.serverError)
        except Exception, ex:
            sError = "DBSReaderModel/listPhysicsGroups. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    def getServerInfo(self):
        """
        FIXME
        Method that provides information about DBS Server to the clients
        The information includes
        * Server Version - SVN tag
        * Schema Version - Version of Schema this DBS instance is working with
        * ETC - TBD
        """
        ret = {}
        ret["tagged_version"] = self.getServerVersion()
        ret["schema"] = self.dbsStatus.getSchemaStatus()
        ret["components"] = self.dbsStatus.getComponentStatus()
        return ret

