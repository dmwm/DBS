from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.DBFormatter import DBFormatter
from WMCore.DAOFactory import DAOFactory

class DBS3SqlApi(object):
    def __init__(self,logger,connectUrl,owner):
        object.__init__(self)
        dbFactory = DBFactory(logger, connectUrl, options={})
        self.dbi = dbFactory.connect()

        daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=self.dbi, owner=owner)

        self.dbFormatter = DBFormatter(logger,self.dbi)

        self.datasetListDAO = daofactory(classname="Dataset.List")
        self.dataTierListDAO = daofactory(classname="DataTier.List")
        self.primaryDatasetListDAO = daofactory(classname="PrimaryDataset.List")
        self.blockListDAO = daofactory(classname="Block.List")
        self.fileListDAO = daofactory(classname="File.List")
                
        self.sqlPrimaryKey = {'Block':'block_id',
                              'Dataset':'dataset_id',
                              'DataTier':'data_tier_id',
                              'FileMinMaxCount':'minimum',
                              'Files':'file_id',
                              'PrimaryDS':'primary_ds_id'}

        self.sqlDict = {'Dataset':
                        """
                        SELECT D.DATASET_ID, D.DATASET,
                        D.IS_DATASET_VALID, D.XTCROSSSECTION,
                        D.CREATION_DATE, D.CREATE_BY,
                        D.LAST_MODIFICATION_DATE, D.LAST_MODIFIED_BY,
                        P.PRIMARY_DS_NAME,
                        PDT.PRIMARY_DS_TYPE,
                        PD.PROCESSED_DS_NAME,
                        DT.DATA_TIER_NAME,
                        DP.DATASET_ACCESS_TYPE,
                        AE.ACQUISITION_ERA_NAME,
                        PE.PROCESSING_VERSION,
                        PH.PHYSICS_GROUP_NAME
                        FROM %s.DATASETS D
                        JOIN %s.PRIMARY_DATASETS P ON P.PRIMARY_DS_ID = D.PRIMARY_DS_ID
                        JOIN %s.PRIMARY_DS_TYPES PDT ON PDT.PRIMARY_DS_TYPE_ID = P.PRIMARY_DS_TYPE_ID
                        JOIN %s.PROCESSED_DATASETS PD ON PD.PROCESSED_DS_ID = D.PROCESSED_DS_ID
                        JOIN %s.DATA_TIERS DT ON DT.DATA_TIER_ID = D.DATA_TIER_ID
                        JOIN %s.DATASET_ACCESS_TYPES DP on DP.DATASET_ACCESS_TYPE_ID= D.DATASET_ACCESS_TYPE_ID
                        LEFT OUTER JOIN %s.ACQUISITION_ERAS AE ON AE.ACQUISITION_ERA_ID = D.ACQUISITION_ERA_ID
                        LEFT OUTER JOIN %s.PROCESSING_ERAS PE ON PE.PROCESSING_ERA_ID = D.PROCESSING_ERA_ID
                        LEFT OUTER JOIN %s.PHYSICS_GROUPS PH ON PH.PHYSICS_GROUP_ID = D.PHYSICS_GROUP_ID
                        """ % ((owner,)*9),
                        'FileMinMaxCount':
                        """SELECT MIN(F.FILE_ID) MINIMUM, MAX(F.FILE_ID) MAXIMUM, COUNT(*) COUNT FROM %s.FILES F""" % (owner),
                        ##############################################
                        'Files':
                        """SELECT F.FILE_ID, F.LOGICAL_FILE_NAME, F.IS_FILE_VALID,
                        F.DATASET_ID, D.DATASET,
                        F.BLOCK_ID, B.BLOCK_NAME,
                        F.FILE_TYPE_ID, FT.FILE_TYPE,
                        F.CHECK_SUM, F.EVENT_COUNT, F.FILE_SIZE, 
                        F.BRANCH_HASH_ID, F.ADLER32, F.MD5,
                        F.AUTO_CROSS_SECTION,
                        F.CREATION_DATE, F.CREATE_BY,
                        F.LAST_MODIFICATION_DATE, F.LAST_MODIFIED_BY
                        FROM %s.FILES F
                        JOIN %s.FILE_DATA_TYPES FT ON  FT.FILE_TYPE_ID = F.FILE_TYPE_ID
                        JOIN %s.DATASETS D ON  D.DATASET_ID = F.DATASET_ID
                        JOIN %s.BLOCKS B ON B.BLOCK_ID = F.BLOCK_ID
                        WHERE F.FILE_ID BETWEEN :minID AND :maxID
                        """ % (owner,owner,owner,owner)
                        }
        
    def blockList(self,sort=True):
        result = self.blockListDAO.execute(self.dbi.connection())
        return sorted(result,key=lambda entry: entry[self.sqlPrimaryKey['Block']])

    def datasetList(self,sort=True):
        #result = self.datasetListDAO.execute(self.dbi.connection())
        #return sorted(result,key=lambda entry: entry[self.sqlPrimaryKey['Dataset']])
        binds = {}
        return self._queryDB('Dataset',binds=binds,sort=sort) 

    def dataTierList(self,sort=True):
        result = self.dataTierListDAO.execute(self.dbi.connection(),None)
        return sorted(result,key=lambda entry: entry[self.sqlPrimaryKey['DataTier']])

    def fileMinMaxCount(self,sort=False):
        return self._queryDB('FileMinMaxCount',sort=sort)

    def fileList(self,minimum,maximum,sort=True):
        binds = {'minID':minimum,'maxID':maximum}
        
        return self._queryDB('Files',binds=binds,sort=sort)

    def primaryDatasetList(self,sort=True):
        result = self.primaryDatasetListDAO.execute(self.dbi.connection())
        return sorted(result,key=lambda entry: entry[self.sqlPrimaryKey['PrimaryDS']])

    def _queryDB(self,query,binds={},sort=True):
        connection = self.dbi.connection()
        
        cursors = self.dbi.processData(self.sqlDict[query],
                                      binds,
                                      connection,
                                      transaction=False,
                                      returnCursor=True)

        result = self.dbFormatter.formatCursor(cursors[0])

        connection.close()
        
        if sort:
            return sorted(result,key=lambda entry: entry[self.sqlPrimaryKey[query]])
        else:
            return result
