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

from cherrypy import request, tools

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
from dbs.business.DBSDataTier import DBSDataTier
from dbs.business.DBSStatus import DBSStatus
from dbs.business.DBSMigrate import DBSMigrate
from dbs.business.DBSBlockInsert import DBSBlockInsert
from dbs.business.DBSReleaseVersion import DBSReleaseVersion
from dbs.business.DBSDatasetAccessType import DBSDatasetAccessType
from dbs.business.DBSPhysicsGroup import DBSPhysicsGroup
from dbs.utils.dbsException import dbsException, dbsExceptionCode
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler

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
        self.methods = {'GET':{}, 'PUT':{}, 'POST':{}, 'DELETE':{}}
        self._addMethod('GET', 'serverinfo', self.getServerInfo)
        #self._addMethod('GET', 'donothing', self.donothing)
        self._addMethod('GET', 'primarydatasets', self.listPrimaryDatasets, args=['primary_ds_name', 'primary_ds_type'])
        self._addMethod('GET', 'primarydstypes', self.listPrimaryDsTypes, args=['primary_ds_type', 'dataset'])
        self._addMethod('GET', 'datasets', self.listDatasets, args=['dataset', 'parent_dataset', 'release_version',
                                'pset_hash', 'app_name', 'output_module_label', 'processing_version',
                                'acquisition_era_name', 'run_num','physics_group_name', 'logical_file_name',
                                'primary_ds_name', 'primary_ds_type', 'data_tier_name', 'dataset_access_type',
                                'is_dataset_valid', 'min_cdate, max_cdate', 'min_ldate', 'max_ldate',
                                'cdate', 'ldate', 'detail'])
        self._addMethod('POST', 'datasetlist', self.listDatasetArray)
        self._addMethod('GET', 'blocks', self.listBlocks, args=['dataset', 'block_name', 'origin_site_name',
                        'logical_file_name', 'run_num', 'min_cdate', 'max_cdate', 'min_ldate',
                        'max_ldate', 'cdate', 'ldate', 'detail'])
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
        self._addMethod('GET', 'filechildren', self.listFileChildren, args=['logical_file_name'])
        self._addMethod('GET', 'filelumis', self.listFileLumis, args=['logical_file_name', 'block_name', 'run_num'])
        self._addMethod('GET', 'runs', self.listRuns, args=['minrun', 'maxrun', 'logical_file_name',
                        'block_name', 'dataset'])
        self._addMethod('GET', 'datatypes', self.listDataTypes, args=['datatype', 'dataset'])
        self._addMethod('GET', 'datatiers', self.listDataTiers, args=['data_tier_name'])
        self._addMethod('GET', 'blockparents', self.listBlockParents, args=['block_name'])
        self._addMethod('POST', 'blockparents', self.listBlocksParents)
        self._addMethod('GET', 'blockchildren', self.listBlockChildren, args=['block_name'])
        self._addMethod('GET', 'blockdump', self.dumpBlock, args=['block_name'])
        self._addMethod('GET', 'acquisitioneras', self.listAcquisitionEras, args=['acquisition_era_name'])
        self._addMethod('GET', 'processingeras', self.listProcessingEras, args=['processing_version'])
        self._addMethod('GET', 'releaseversions', self.listReleaseVersions, args=['release_version', 'dataset'])
        self._addMethod('GET', 'datasetaccesstypes', self.listDatasetAccessTypes, args=['dataset_access_type'])
        self._addMethod('GET', 'physicsgroups', self.listPhysicsGroups, args=['physics_group_name'])
        self._addMethod('GET', 'help', self.getHelp, args=['call'])

        self.dbsDoNothing = DBSDoNothing(self.logger, self.dbi, config.dbowner)
        self.dbsPrimaryDataset = DBSPrimaryDataset(self.logger, self.dbi, config.dbowner)
        self.dbsDataset = DBSDataset(self.logger, self.dbi, config.dbowner)
        self.dbsBlock = DBSBlock(self.logger, self.dbi, config.dbowner)
        self.dbsFile = DBSFile(self.logger, self.dbi, config.dbowner)
        self.dbsAcqEra = DBSAcquisitionEra(self.logger, self.dbi,
            config.dbowner)
        self.dbsOutputConfig = DBSOutputConfig(self.logger, self.dbi,
            config.dbowner)
        self.dbsProcEra = DBSProcessingEra(self.logger, self.dbi,
            config.dbowner)
        self.dbsSite = DBSSite(self.logger, self.dbi, config.dbowner)
        self.dbsRun = DBSRun(self.logger, self.dbi, config.dbowner)
        self.dbsDataType = DBSDataType(self.logger, self.dbi, config.dbowner)
        self.dbsDataTier = DBSDataTier(self.logger, self.dbi, config.dbowner)
        self.dbsStatus = DBSStatus(self.logger, self.dbi, config.dbowner)
        self.dbsMigrate = DBSMigrate(self.logger, self.dbi, config.dbowner)
        self.dbsBlockInsert = DBSBlockInsert(self.logger, self.dbi, config.dbowner) 
        self.dbsReleaseVersion = DBSReleaseVersion(self.logger, self.dbi, config.dbowner)
        self.dbsDatasetAccessType = DBSDatasetAccessType(self.logger, self.dbi, config.dbowner)
        self.dbsPhysicsGroup = DBSPhysicsGroup(self.logger, self.dbi, config.dbowner)
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
            params = inspect.getargspec(self.methods['GET'][call]['call'])[0]
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

    """
    Used for Stress test. 
    def donothing(self):
        return self.dbsDoNothing.listNone()
    """
    @tools.secmodv2()
    def listPrimaryDatasets(self, primary_ds_name="", primary_ds_type=""):
        """
        Example url's: <br />
        http://dbs3/primarydatasets <br />
        http://dbs3/primarydatasets/qcd_20_30 <br />
        http://dbs3/primarydatasets?primaryDSName=qcd* <br />
        http://dbs3/primarydatasets?primaryDSType=qcd* <br />
        """
        #import pdb
        #pdb.set_trace()
        primary_ds_name = primary_ds_name.replace("*","%")
        primary_ds_type = primary_ds_type.replace("*","%")
        try:
            #print"-----ListPrimaryDatasets___"
            return self.dbsPrimaryDataset.listPrimaryDatasets(primary_ds_name, primary_ds_type)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except Exception, ex:
            sError = "DBSReaderModel/listPrimaryDatasets. %s\n Exception trace: \n %s." \
                    % (ex, traceback.format_exc() )
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)
    @tools.secmodv2()
    def listPrimaryDsTypes(self, primary_ds_type="", dataset=""):
        """
        Example URL's <br />
        http://dbs3/primarydstypes <br />
        http://dbs3/primarydstypes?primary_ds_type=qcd* <br />
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
    @tools.secmodv2()
    def listDatasets(self, dataset="", parent_dataset="", is_dataset_valid=1,
        release_version="", pset_hash="", app_name="", output_module_label="",
        processing_version="", acquisition_era_name="", run_num="0",
        physics_group_name="", logical_file_name="", primary_ds_name="",
        primary_ds_type="", data_tier_name="", dataset_access_type="RO",
        min_cdate='0', max_cdate='0', min_ldate='0', max_ldate='0', cdate='0',
        ldate='0', detail=False):
        #import pdb
        #pdb.set_trace()
        """
        This API lists the dataset paths and associated information.
        If no parameter is given, all datasets will be returned.
        <dataset> parameter can include one or several '*' as wildcards.
        <detail> parameter is defaulted to False, which means only
        dataset paths will be returned in the output dictionary. 
        In order to get more information, one needs to provide detail=True.
        <run_num> can be only be passed as a single number. No interval of
        run numbers is supported for this api for now.
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
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except Exception, ex:
            sError = "DBSReaderModel/listDatasets.  %s \n. Exception trace: \n %s" \
                % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)
        #
        detail = detail in (True, 1, "True", "1", 'true')
        try:
            return self.dbsDataset.listDatasets(dataset, parent_dataset, is_dataset_valid, release_version, pset_hash,
                app_name, output_module_label, processing_version, acquisition_era_name,
                run_num, physics_group_name, logical_file_name, primary_ds_name, primary_ds_type,
                data_tier_name, dataset_access_type,
                min_cdate, max_cdate, min_ldate, max_ldate, cdate, ldate, detail)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except Exception, ex:
            sError = "DBSReaderModel/listdatasets. %s.\n Exception trace: \n %s" % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @tools.secmodv2()
    def listDatasetArray(self):
        """
        To be called by datasets url with post call
        """
        try :
            body = request.body.read()
            data = cjson.decode(body)
            #import pdb
            #pdb.set_trace()
            return self.dbsDataset.listDatasetArray(data)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except Exception, ex:
            sError = "DBSReaderModel/listDatasetArray. %s \n Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)
 
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
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except Exception, ex:
            sError = "DBSReaderModel/listDataTiers. %s\n. Exception trace: \n %s" \
                    % ( ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @tools.secmodv2()
    def listBlocks(self, dataset="", block_name="", origin_site_name="",
        logical_file_name="",run_num='-1', min_cdate='0', max_cdate='0',
        min_ldate='0', max_ldate='0', cdate='0',  ldate='0', detail=False):
        """
        Example url's:
        http://dbs3/blocks?dataset=myDataset ||?origin_site_name=mySite <br />
        http://dbs3/blocks?block_name=myBlock ||?origin_site_name=mySite <br />
        http://dbs3/blocks?logical_file_name=my_lfn ||?origin_site_name=mySite<br />
        http://dbs3/blocks?logical_file_name=my_lfn*?dataset=myDataset*?block_name=myBlock ||?origin_site_name=mySite<br />
        """
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
        except Exception, ex:
            sError = "DBSReaderModel/listBlocks.\n. %s \n Exception trace: \n %s" \
                                % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-invalid-input2',  str(ex), self.logger.exception, sError )
        detail = detail in (True, 1, "True", "1", 'true')
        try:
            return self.dbsBlock.listBlocks(dataset, block_name, origin_site_name, logical_file_name, run_num,
                min_cdate, max_cdate, min_ldate, max_ldate, cdate, ldate, detail)

        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message) 
        except Exception, ex:
            sError = "DBSReaderModel/listBlocks. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @tools.secmodv2()
    def listBlockParents(self, block_name=""):
        """
        Example url's:
        http://dbs3/blockparents?block_name=/a/b/c%23*d <br />
        """
        try:
            return self.dbsBlock.listBlockParents(block_name)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except Exception, ex:
            sError = "DBSReaderModel/listBlockParents. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error',  dbsExceptionCode['dbsException-server-error'],  self.logger.exception, sError)

    @tools.secmodv2()
    def listBlocksParents(self):
        """
        To be called by blockparents url with post call
        """
        try :
            body = request.body.read()
            data = cjson.decode(body)
            return self.dbsBlock.listBlockParents(data["block_name"])
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except cjson.DecodeError, de:
            sError = "DBSReaderModel/listBlockParents. %s\n. Exception trace: \n %s" \
                    % (de, traceback.format_exc())
            msg = "DBSReaderModel/listBlockParents. %s" % de
            dbsExceptionHandler('dbsException-invalid-input2', msg, self.logger.exception, sError)
        except Exception, ex:
            sError = "DBSReaderModel/listBlockParents. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)
            
    @tools.secmodv2()
    def listBlockChildren(self, block_name=""):
        """
        Example url's:
        http://dbs3/blockchildren?block_name=/a/b/c%23*d <br />
        """
        block_name = block_name.replace("*","%")
        try:
            return self.dbsBlock.listBlockChildren(block_name)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except Exception, ex:
            sError = "DBSReaderModel/listBlockChildren. %s\n. Exception trace: \n %s" % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @tools.secmodv2()
    def listFiles(self, dataset = "", block_name = "", logical_file_name = "",
        release_version="", pset_hash="", app_name="", output_module_label="",
        minrun=-1, maxrun=-1, origin_site_name="", lumi_list="", detail=False):
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

        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except Exception, ex:
            sError = "DBSReaderModel/listFiles. %s \n Exception trace: \n %s" % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'],
                    self.logger.exception, sError)

    @tools.secmodv2()
    def listFileSummaries(self, block_name='', dataset='', run_num=0):
        """
        Example url's <br />
        http://dbs3/filesummaries?dataset=/a/b/c
        http://dbs3/filesummaries?block_name=/a/b/c#1234
        Both block_name and dataset will not allow wildcards.
        Return: number of files, event counts and number of lumi sections in a given block or dataset. 
        """
        try:
            return self.dbsFile.listFileSummary(block_name, dataset, run_num)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except Exception, ex:
            sError = "DBSReaderModel/listFileSummaries. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @tools.secmodv2()
    def listDatasetParents(self, dataset=''):
        """
        Example url's <br />
        http://dbs3/datasetparents?dataset=/a/b/c
        """
        try:
            return self.dbsDataset.listDatasetParents(dataset)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except Exception, ex:
            sError = "DBSReaderModel/listDatasetParents. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)
   
    @tools.secmodv2()
    def listDatasetChildren(self, dataset):
        """
        Example url's <br />
        http://dbs3/datasetchildren?dataset=/a/b/c
        """
        try:
            return self.dbsDataset.listDatasetChildren(dataset)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except Exception, ex:
            sError = "DBSReaderModel/listDatasetChildren. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)
   
    @tools.secmodv2()
    def listOutputConfigs(self, dataset="", logical_file_name="", 
                          release_version="", pset_hash="", app_name="",
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
            return self.dbsOutputConfig.listOutputConfigs(dataset, 
                logical_file_name, release_version, pset_hash, app_name,
                output_module_label, block_id, global_tag)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except Exception, ex:
            sError = "DBSReaderModel/listOutputConfigs. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)
    
    @tools.secmodv2()
    def listFileParents(self, logical_file_name='', block_id=0, block_name=''):
        """
        Example url's <br />
        http://dbs3/fileparents?logical_file_name=lfn
        """
        try:
            return self.dbsFile.listFileParents(logical_file_name, block_id, block_name)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except Exception, ex:
            sError = "DBSReaderModel/listFileParents. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @tools.secmodv2()
    def listFileChildren(self, logical_file_name=''):
        """
        Example url's <br />
        http://dbs3/filechildren?logical_file_name=lfn
        """
        try:
            return self.dbsFile.listFileChildren(logical_file_name)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except Exception, ex:
            sError = "DBSReaderModel/listFileChildren. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)
    
    @tools.secmodv2()
    def listFileLumis(self, logical_file_name="", block_name="", run_num='0'):
        """
        Example url's <br />
        http://dbs3/filelumis?logical_file_name=lfn
        http://dbs3/filelumis?block_name=block_name
        """
        if '*' in run_num or '%' in run_num:
            run_num = 0
        else:
            run_num = int(run_num)
        try:
            return self.dbsFile.listFileLumis(logical_file_name, block_name, run_num )
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except Exception, ex:
            sError = "DBSReaderModel/listFileLumis. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @tools.secmodv2()     
    def listRuns(self, minrun=-1, maxrun=-1, logical_file_name="",
                 block_name="", dataset=""):
        """
        http://dbs3/runs?runmin=1&runmax=10
        http://dbs3/runs
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
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except Exception, ex:
            sError = "DBSReaderModel/listRun. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    #def listSites(self, block_name="", site_name=""):
    #    """
    #    Example url's <br />
    #    http://dbs3/sites
    #   http://dbs3/sites?block_name=block_name
    #   http://dbs3/sites?site_name=T1_FNAL
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
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except Exception, ex:
            sError = "DBSReaderModel/listDataTypes. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)
    
    @tools.secmodv2()
    def dumpBlock(self, block_name):
        """
        Returns all information related with the block_name
        """
        try:
            return self.dbsMigrate.dumpBlock(block_name)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except Exception, ex:
            sError = "DBSReaderModel/dumpBlock. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @tools.secmodv2()
    def listAcquisitionEras(self, acquisition_era_name=''):
        """
        lists acquisition eras known to dbs
        """
        try:
            acquisition_era_name = acquisition_era_name.replace('*', '%')
            return  self.dbsAcqEra.listAcquisitionEras(acquisition_era_name)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except Exception as ex:
            sError = "DBSReaderModel/listAcquisitionEras. %s\n. Exception trace: \n %s" % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @tools.secmodv2()
    def listProcessingEras(self, processing_version=''):
        """
        lists acquisition eras known to dbs
        """
        try:
            processing_version = processing_version.replace("*", "%")
            return  self.dbsProcEra.listProcessingEras(processing_version)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except Exception, ex:
            sError = "DBSReaderModel/listProcessingEras. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @tools.secmodv2()
    def listReleaseVersions(self, release_version='', dataset=''):
        """
        lists release versions known to dbs
        """
        if release_version:
            release_version = release_version.replace("*","%")
        try:
            return self.dbsReleaseVersion.listReleaseVersions(release_version, dataset )
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except Exception, ex:
            sError = "DBSReaderModel/listReleaseVersions. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)
    
    @tools.secmodv2()
    def listDatasetAccessTypes(self, dataset_access_type=''):
        """
        lists dataset access types known to dbs
        """
        if dataset_access_type:
            dataset_access_type = dataset_access_type.replace("*","%")
        try:
            return  self.dbsDatasetAccessType.listDatasetAccessTypes(dataset_access_type)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except Exception, ex:
            sError = "DBSReaderModel/listDatasetAccessTypes. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)

    @tools.secmodv2()
    def listPhysicsGroups(self, physics_group_name=''):
        """
        List physics group names know to DBS.
        """
        if physics_group_name:
            physics_group_name = physics_group_name.replace('*', '%')
        try:
            return self.dbsPhysicsGroup.listPhysicsGroups(physics_group_name)
        except dbsException as de:
            dbsExceptionHandler(de.eCode, de.message, self.logger.exception, de.message)
        except Exception, ex:
            sError = "DBSReaderModel/listPhysicsGroups. %s\n. Exception trace: \n %s" \
                    % (ex, traceback.format_exc())
            dbsExceptionHandler('dbsException-server-error', dbsExceptionCode['dbsException-server-error'], self.logger.exception, sError)
