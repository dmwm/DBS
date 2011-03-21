from WMCore.Database.DBFactory import DBFactory
from WMCore.Database.DBFormatter import DBFormatter

def modifyDatasetList(datasetList):
    ### DBS3 has additional fields
    for dataset in datasetList:
        dataset['dataset'] = '/'+dataset['primary_ds_name']+'/'+dataset['processed_ds_name']+'/'+dataset['data_tier_name']
        dataset['is_dataset_valid'] = int(dataset['status'] in ('VALID', 'RO', 'PRODUCTION', 'IMPORTED', 'EXPORTED'))

        if dataset['status'] in ('VALID'):
            dataset['dataset_access_type'] = 'UNKNOWN_DBS2_TYPE'
        else:
            dataset['dataset_access_type'] = dataset['status']
            
        dataset['processing_version'] = None
        del dataset['status']
        
    return datasetList

def modifyBlocklist(blockList):
    for block in blockList:
        if block['origin_site_name'] == 'unknown':
            block['origin_site_name'] = 'unknow'
    
    return blockList

class DBS2SqlApi(object):
    def __init__(self,logger,connectUrl,owner):
        object.__init__(self)
        dbFactory = DBFactory(logger, connectUrl, options={})
        self.dbi = dbFactory.connect()
        self.dbFormatter = DBFormatter(logger,self.dbi)

        self.sqlDict = {'Block':
                        """SELECT BL.ID block_id,
                        BL.NAME block_name,
                        BL.PATH dataset,
                        BL.BLOCKSIZE block_size,
                        BL.NUMBEROFFILES file_count,
                        BL.OPENFORWRITING open_for_writing,
                        PS1.DISTINGUISHEDNAME CREATE_BY,
                        PS2.DISTINGUISHEDNAME last_modified_by,
                        BL.LASTMODIFICATIONDATE last_modification_date ,
                        BL.CREATIONDATE creation_date,
                        BL.DATASET dataset_id, '/' || PD.NAME || '/' || DS.NAME || '/' || DT.NAME dataset,
                        'unknown' origin_site_name
                        FROM %s.BLOCK BL
                        JOIN %s.PERSON PS1 ON BL.CREATEDBY=PS1.ID
                        JOIN %s.PERSON PS2 ON BL.LASTMODIFIEDBY=PS2.ID
                        JOIN %s.PROCESSEDDATASET DS ON DS.ID=BL.DATASET
                        JOIN %s.PRIMARYDATASET PD on DS.PRIMARYDATASET=PD.ID
                        JOIN %s.DATATIER DT ON DS.DATATIER=DT.ID""" % (owner,owner,owner,owner,owner,owner),
                        ##############################################
                        'Dataset':
                        """SELECT DS.ID dataset_id,
                        DS.NAME processed_ds_name,
                        PD.NAME primary_ds_name,
                        DT.NAME data_tier_name,
                        PG.PHYSICSGROUPNAME physics_group_name,
                        ST.STATUS,
                        DS.AQUISITIONERA acquisition_era_name,
                        DS.XTCROSSSECTION,
                        PS1.DISTINGUISHEDNAME CREATE_BY,
                        DS.CREATIONDATE creation_date,
                        PS2.DISTINGUISHEDNAME LAST_MODIFIED_BY,
                        DS.LASTMODIFICATIONDATE last_modification_date,
                        PT.TYPE primary_ds_type
                        FROM %s.PROCESSEDDATASET DS
                        JOIN %s.DATATIER DT ON DS.DATATIER=DT.ID
                        JOIN %s.PRIMARYDATASET PD ON PD.ID=DS.PRIMARYDATASET
                        JOIN %s.PHYSICSGROUP PG ON PG.ID=DS.PHYSICSGROUP
                        JOIN %s.PROCDSSTATUS ST ON ST.ID=DS.STATUS
                        JOIN %s.PERSON PS1 ON DS.CREATEDBY=PS1.ID
                        JOIN %s.PERSON PS2 ON DS.LASTMODIFIEDBY=PS2.ID
                        JOIN %s.PRIMARYDSTYPE PT ON PT.ID=PD.TYPE""" % (owner,owner,owner,owner,owner,owner,owner,owner),
                        ##############################################
                        'DataTier':
                        """SELECT DT.ID data_tier_id,
                        DT.NAME data_tier_name,
                        PS.DISTINGUISHEDNAME create_by,
                        DT.CREATIONDATE creation_date
                        FROM %s.DATATIER DT
                        JOIN %s.PERSON PS ON PS.ID=DT.CREATEDBY""" % (owner,owner),
                        ##############################################
                        'FileMinMaxCount':
                        """SELECT MIN(FS.ID) MINIMUM, MAX(FS.ID) MAXIMUM, COUNT(*) COUNT FROM %s.FILES FS""" % (owner),
                        ##############################################
                        'Files':
                        """SELECT
                        FS.ID file_id,
                        FS.LOGICALFILENAME logical_file_name,
                        BL.NAME block_name,
                        FS.CHECKSUM check_sum,
                        FS.NUMBEROFEVENTS event_count,
                        FS.FILESIZE file_size,
                        FS.FILETYPE file_type_id,
                        FT.TYPE file_type,
                        FS.VALIDATIONSTATUS is_file_valid,
                        PS1.DISTINGUISHEDNAME create_by,
                        PS2.DISTINGUISHEDNAME last_modified_by,
                        FS.LASTMODIFICATIONDATE last_modification_date,
                        FS.CREATIONDATE creation_date,
                        FS.FILEBRANCH branch_hash_id,
                        FS.AUTOCROSSSECTION auto_cross_section,
                        FS.ADLER32,
                        FS.MD5,
                        '/' || PD.NAME || '/' || DS.NAME || '/' || DT.NAME dataset,
                        FS.DATASET dataset_id,
                        FS.BLOCK block_id
                        FROM %s.FILES FS
                        JOIN %s.PROCESSEDDATASET DS ON DS.ID=FS.DATASET
                        JOIN %s.PRIMARYDATASET PD on DS.PRIMARYDATASET=PD.ID
                        JOIN %s.DATATIER DT ON DS.DATATIER=DT.ID
                        JOIN %s.PERSON PS1 ON FS.CREATEDBY=PS1.ID
                        JOIN %s.PERSON PS2 ON FS.LASTMODIFIEDBY=PS2.ID
                        JOIN %s.BLOCK BL ON FS.BLOCK=BL.ID
                        JOIN %s.FILETYPE FT ON FT.ID=FS.FILETYPE
                        WHERE FS.ID BETWEEN :minID AND :maxID
                        """ % (owner,owner,owner,owner,owner,owner,owner,owner),
                        ##############################################
                        'PrimaryDS':
                        """SELECT PD.ID primary_ds_id,
                        PD.NAME primary_ds_name,
                        PT.TYPE primary_ds_type,
                        PD.CREATIONDATE creation_date,
                        PS.DISTINGUISHEDNAME create_by
                        FROM %s.PRIMARYDATASET PD
                        JOIN %s.PERSON PS ON PS.ID=PD.CREATEDBY
                        JOIN %s.PRIMARYDSTYPE PT ON PT.ID=PD.TYPE""" % (owner,owner,owner)
                        }
                                
        self.sqlPrimaryKey = {'Block':'block_id',
                              'Dataset':'dataset_id',
                              'DataTier':'data_tier_id',
                              'FileMinMaxCount':'minimum',
                              'Files':'file_id',
                              'PrimaryDS':'primary_ds_id'}

    def blockList(self,sort=True):
        return modifyBlocklist(self._queryDB('Block',sort=sort))

    def datasetList(self,sort=True):
        #ToDo mapping DBS2 PROCDSSTATUS to is_dataset_valid and dataset_access_type in DBS3
        #ToDo compare GlobalTAG when comparing output_module_configs
        return modifyDatasetList(self._queryDB('Dataset',sort=sort))

    def dataTierList(self,sort=True):
        return self._queryDB('DataTier',sort=sort)

    def fileMinMaxCount(self,sort=False):
        return self._queryDB('FileMinMaxCount',sort=sort)

    def fileList(self,minimum,maximum,sort=True):
        binds = {'minID':minimum,'maxID':maximum}
        return self._queryDB('Files',binds=binds,sort=sort)
    
    def primaryDatasetList(self,sort=True):
        return self._queryDB('PrimaryDS',sort=sort)

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
