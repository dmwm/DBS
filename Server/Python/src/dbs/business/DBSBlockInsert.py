#!/usr/bin/env python
"""
DBS  block insertion for WMAgent
"""

from sqlalchemy import exceptions
from WMCore.DAOFactory import DAOFactory
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.dbsUtils import dbsUtils

class DBSBlockInsert :
    """
    Insert a Block and everything under this block.
    """

    def __init__(self, logger, dbi, owner):
        daofactory = DAOFactory(package='dbs.dao', logger=logger,
                                dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        # Setup the DAO objects
        self.primdslist     = daofactory(classname="PrimaryDataset.List")
        self.primdsid       = daofactory(classname="PrimaryDataset.GetID")
        self.datasetlist    = daofactory(classname="Dataset.List")
        self.datasetid      = daofactory(classname="Dataset.GetID")
        self.blocklist      = daofactory(classname="Block.List")
        self.blockid        = daofactory(classname="Block.GetID")
        self.filelist       = daofactory(classname="File.List")
        self.fileid         = daofactory(classname="File.GetID")
        self.filetypeid     = daofactory(classname="FileType.GetID")
        self.fplist         = daofactory(classname="FileParent.List")
        self.fllist         = daofactory(classname="FileLumi.List")
        self.primdstpid     = daofactory(classname="PrimaryDSType.GetID")
        self.tierid         = daofactory(classname="DataTier.GetID")
        self.datatypeid     = daofactory(classname="DatasetType.GetID")
        self.phygrpid       = daofactory(classname="PhysicsGroup.GetID")
        self.acqid          = daofactory(classname="AcquisitionEra.GetID")
        self.procsingid     = daofactory(classname="ProcessingEra.GetID")
        self.procdsid       = daofactory(classname="ProcessedDataset.GetID")
        self.otptModCfgid   = daofactory(classname="OutputModuleConfig.GetID")
        self.releaseVid     = daofactory(classname="ReleaseVersion.GetID")
        self.psetHashid     = daofactory(classname="ParameterSetHashe.GetID")
        self.appid          = daofactory(classname="ApplicationExecutable.GetID")
        self.procsingin     = daofactory(classname="ProcessingEra.Insert")
        self.acqin          = daofactory(classname="AcquisitionEra.Insert")
        self.procdsin       = daofactory(classname="ProcessedDataset.Insert")
        self.primdsin       = daofactory(classname="PrimaryDataset.Insert2")
        self.primdstpin     = daofactory(classname="PrimaryDSType.Insert")
        self.datasetin      = daofactory(classname="Dataset.Insert2")
        self.datatypein     = daofactory(classname="DatasetType.Insert")
        self.blockin        = daofactory(classname="Block.Insert")
        self.sm             = daofactory(classname="SequenceManager")
        self.filein         = daofactory(classname="File.Insert2")
        self.flumiin        = daofactory(classname="FileLumi.Insert")
        self.fparentin      = daofactory(classname="FileParent.Insert")
        self.fpbdlist       = daofactory(classname="FileParentBlock.List")
        self.blkparentin2    = daofactory(classname="BlockParent.Insert2")
        self.dsparentin2     = daofactory(classname="DatasetParent.Insert2")
        self.blkstats       = daofactory(classname="Block.ListStats")
        self.blkstatsin     = daofactory(classname="Block.UpdateStats")
        self.fconfigin      = daofactory(classname=
                                            "FileOutputMod_config.Insert")
        self.bufdeletefiles = daofactory(classname="FileBuffer.DeleteFiles")
        self.buflist        = daofactory(classname="FileBuffer.List")
        self.buflistblks    = daofactory(classname="FileBuffer.ListBlocks")
        self.buffinddups    = daofactory(classname="FileBuffer.FindDuplicates")
        self.bufdeletedups  = daofactory(classname=
                                            "FileBuffer.DeleteDuplicates")
        self.compstatusin   = daofactory(classname="ComponentStatus.Insert")
        self.compstatusup   = daofactory(classname="ComponentStatus.Update")
        self.otptModCfgin   = daofactory(classname="OutputModuleConfig.Insert")
        self.releaseVin     = daofactory(classname="ReleaseVersion.Insert")
        self.psetHashin     = daofactory(classname="ParameterSetHashe.Insert")
        self.appin          = daofactory(classname=
                                            "ApplicationExecutable.Insert")
        self.dcin           = daofactory(classname=
                                            "DatasetOutputMod_config.Insert")
        self.phygrpin       = daofactory(classname="PhysicsGroup.Insert")
        #self.newBlock = False

        # Add a cache that Anzar thinks he needs.
        # Cache is only needed for output module configuration since it is
        # shared between file and dataset.
        # Others go to db in different jobs. YG 11/17/2010
        self.datasetCache = {'conf': {}}

    def putBlock(self, blockcontent, migration=False):
        """
        Insert the data in sereral steps and commit when each step finishes or rollback if there is a problem.
        """
        #YG
        try:
            #1 insert configuration
            self.logger.debug("insert configuration")
            configList = self.insertOutputModuleConfig(
                            blockcontent['dataset_conf_list'], migration)
            #2 insert dataset
            self.logger.debug("insert dataset")
            datasetId = self.insertDataset(blockcontent, configList, migration)
            #3 insert block & files
            self.logger.debug("insert block & files.")
            self.insertBlockFile(blockcontent, datasetId, migration)
        except Exception, ex:
            raise

    def insertBlockFile(self, blockcontent, datasetId, migration=False):

        block = blockcontent['block']
        newBlock = False
        #Insert the block
        conn = self.dbi.connection()
        tran = conn.begin()
        self.logger.info("Inserted block name: %s" %block['block_name'])
        try:
            block['block_id'] = self.sm.increment(conn, "SEQ_BK",)
            block['dataset_id'] =  datasetId
            block['creation_date'] = block.get('creation_date', dbsUtils().getTime())
	    if not migration:
		block['create_by'] = dbsUtils().getCreateBy()
		block['last_modification_date'] = dbsUtils().getTime()
		block['last_modified_by'] = dbsUtils().getCreateBy()
            self.blockin.execute(conn, block, tran)
            newBlock = True
        except exceptions.IntegrityError, ex:
            if (str(ex).find("ORA-00001") != -1 and str(ex).find("TUC_BK_BLOCK_NAME") != -1) or str(ex).lower().find("duplicate") != -1:
            #not sure what happends to WMAgent: Does it try to insert a
            #block again? YG 10/05/2010
            #Talked with Matt N: We should stop insertng this block now.
            #This means there is some trouble.
            #Throw exception to let the up layer know. YG 11/17/2010
                if tran:tran.rollback()
                if conn:conn.close()
                dbsExceptionHandler("dbsException-invalid-input2","DBSBlockInsert/insertBlock. Block %s already exists." % (block['block_name']))
            else:
                if tran:tran.rollback()
                if conn:conn.close()
                raise
        #All Praentage will be deduced from file parentage.
        blockId = block['block_id']
        fileLumiList = []
        fileConfObjs = []
        logicalFileName = {}
        fileList = blockcontent['files']
        fileConfigList = blockcontent['file_conf_list']
        if blockcontent.has_key('file_parent_list'):
            fileParentList = blockcontent['file_parent_list']
        else:
            fileParentList = []
        if not fileList:
            return
        intval = 40
        try:
            for i in range(len(fileList)):
                if (i % intval == 0):
                    id = self.sm.increment(conn, "SEQ_FL", False, intval)
                fileList[i]['file_id'] = id
                logicalFileName[fileList[i]['logical_file_name']] = id
                fileList[i]['block_id'] = blockId
                fileList[i]['dataset_id'] = datasetId
                #other fields. YG 11/23/2010
                #print fileList[i]['logical_file_name']
                fileList[i]['is_file_valid'] = fileList[i].get('is_file_valid', 1)
                fileList[i]['check_sum'] = fileList[i].get('check_sum', None)
                fileList[i]['event_count'] = fileList[i].get('event_count', -1)
                fileList[i]['file_size'] = fileList[i].get('file_size', -1)
                fileList[i]['adler32'] = fileList[i].get('adler32', None)
                fileList[i]['md5'] = fileList[i].get('md5', None)
                fileList[i]['auto_cross_section'] = fileList[i].get('auto_cross_section', None)
                #fileList[i]['creation_date'] = fileList[i].get('creation_date', None) #see ticket 965
                #fileList[i]['create_by'] = fileList[i].get('create_by', None)
                #fileList[i]['last_modification_date'] = fileList[i].get('last_modification_date', dbsUtils().getTime())
                #fileList[i]['last_modified_by'] = fileList[i].get('last_modified_by', dbsUtils().getCreateBy())
                fileList[i]['last_modification_date'] = dbsUtils().getTime()
                fileList[i]['last_modified_by'] = dbsUtils().getCreateBy()

                #get lumi info
                lumi = fileList[i]['file_lumi_list']
                nlumi = len(lumi)
                for j in range(nlumi):
                    lumi[j]['file_id'] = id
                fileLumiList[len(fileLumiList):] = lumi
                #remove the lumi list from the file
                del fileList[i]['file_lumi_list']
                id += 1
        except Exception, ex:
            if tran:tran.rollback()
            if conn:conn.close()
            raise
        try:
            #deal with file parentage.
            #At the meantime, we need to build block and dataset parentage.
            #All parentage is deduced from file paretage,
            #10/16/2011
            nfileparent = len(fileParentList)
            bkParentList = []
            dsParentList = []
            for k in range(nfileparent):
                if migration:
                    fileParentList[k]['this_file_id'] = logicalFileName[fileParentList[k]['this_logical_file_name']]
                    del fileParentList[k]['this_logical_file_name']
                    del fileParentList[k]['parent_file_id']
                else:
                    fileParentList[k]['this_file_id'] = logicalFileName[fileParentList[k]['logical_file_name']]
                    del fileParentList[k]['logical_file_name']
                bkParentage2insert={'this_block_id' : blockId, 'parent_logical_file_name': fileParentList[k]['parent_logical_file_name']}
                dsParent2Insert = {'this_dataset_id' : datasetId, 'parent_logical_file_name': fileParentList[k]['parent_logical_file_name']}
                if not any(d.get('parent_logical_file_name') == bkParentage2insert['parent_logical_file_name'] for d in bkParentList):
                    #not exist yet
                    bkParentList.append(bkParentage2insert)
                    dsParentList.append(dsParent2Insert)

            #deal with file config
            for fc in fileConfigList:
                key = (fc['app_name'] + ':' + fc['release_version'] + ':' +
                       fc['pset_hash'] + ':' +
                       fc['output_module_label'] + ':' + fc['global_tag'])
                if not key in (self.datasetCache['conf']).keys():
                    #we expect the config is inserted when the dataset is in.
                    if tran:tran.rollback()
                    if conn:conn.close()
                    dbsExceptionHandler('dbsException-missing-data', 'Required Configuration application name, release version,\
                        pset hash and global tag: %s, %s\
                        ,%s,%s not found in DB' %(fc['app_name'], fc['release_version'], fc['pset_hash'], fc['global_tag']))
                fcObj = {'file_id' : logicalFileName[fc['lfn']],
                         'output_mod_config_id': self.datasetCache['conf'][key]}
                fileConfObjs.append(fcObj)
        except Exception, ex:
            if tran:tran.rollback()
            if conn: conn.close()
            raise
        try:
            #now we build everything to insert the files.
            #tran = conn.begin()
            #insert files
            if fileList:
                self.filein.execute(conn, fileList, tran)
            #insert file parents
            if fileParentList:
                self.fparentin.execute(conn, fileParentList, tran)
            #insert file lumi
            if fileLumiList:
                self.flumiin.execute(conn, fileLumiList, tran)
            #insert file configration
            if fileConfObjs:
                self.fconfigin.execute(conn, fileConfObjs, tran)
            #insert bk and dataset parentage
            #we cannot do bulk insertion for the block and dataset parentage because they may be duplicated.
            lbk = len(bkParentList)
            dsk = len(dsParentList)
            for k in range(lbk):
                try:
                    self.blkparentin2.execute(conn, bkParentList[k], transaction=tran)
                except exceptions.IntegrityError, ex:
                    if (str(ex).find("ORA-00001") != -1 and str(ex).find("PK_BP"))\
                         or str(ex).lower().find("duplicate") != -1:
                        pass
                    elif str(ex).find("ORA-01400") != -1:
                        if tran:tran.rollback()
                        if conn:conn.close()
                        raise
                    else:
                        if tran:tran.rollback()
                        if conn:conn.close()
                        raise
            for k in range(dsk):
                try:
                    self.dsparentin2.execute(conn, dsParentList[k], transaction=tran)
                except exceptions.IntegrityError, ex:
                    if (str(ex).find("ORA-00001") != -1 and str(ex).find("PK_DP"))\
                        or str(ex).lower().find("duplicate") != -1:
                        pass
                    elif str(ex).find("ORA-01400") != -1:
                        if tran:tran.rollback()
                        if conn:conn.close()
                        dbsExceptionHandler("dbsException-missing-data","Failed to insert file. IntegrityError in DB", self.log, str(ex))
                    else:
                        if tran:tran.rollback()
                        if conn:conn.close()
                        raise
            #finally, commit everything for file.
            if tran:tran.commit()
            if conn:conn.close()
        finally:
            if tran:tran.rollback()
            if conn:conn.close()

    def insertOutputModuleConfig(self, remoteConfig, migration=False):
        """
        Insert Release version, application, parameter set hashes and the map(output module config).

        """
        otptIdList = []
        missingList = []
        conn = self.dbi.connection()
        try:
            for c in remoteConfig:
                cfgid = self.otptModCfgid.execute(conn, app = c["app_name"],
                                      release_version = c["release_version"],
                                      pset_hash = c["pset_hash"],
                                      output_label = c["output_module_label"],
                                      global_tag=c['global_tag'])
                if cfgid <= 0 :
                    missingList.append(c)
                else:
                    key = (c['app_name'] + ':' + c['release_version'] + ':' +
                           c['pset_hash'] + ':' +
                           c['output_module_label'] + ':' + c['global_tag'])
                    self.datasetCache['conf'][key] = cfgid
                    otptIdList.append(cfgid)
                    #print "About to set cfgid: %s" % str(cfgid)
        except Exception, ex:
            if conn:conn.close()
            raise

        if len(missingList)==0:
            if conn:conn.close()
            return otptIdList
        #Now insert the missing configs
        try:
            #tran = conn.begin()
            for m in missingList:
                # Start a new transaction
                # This is to see if we can get better results
                # by committing early if we're submitting
                # multiple blocks with similar features
                tran = conn.begin()
                #Now insert the config
                # Sort out the mess
                # We're having some problems with different threads
                # committing different pieces at the same time
                # This makes the output module config ID wrong
                # Trying to catch this via exception handling on duplication
                # Start a new transaction
                #global_tag is now required. YG 03/08/2011
                try:
                    cfgid = 0
		    if not migration:
			m['create_by'] = dbsUtils().getCreateBy()
			m['creation_date'] = dbsUtils().getTime()
                    configObj = {"release_version": m["release_version"],
                                 "pset_hash": m["pset_hash"], "pset_name":m.get('pset_name', None),
                                 "app_name": m["app_name"],
                                 'output_module_label' : m['output_module_label'],
                                 'global_tag' : m['global_tag'],
                                 'scenario' : m.get('scenario', None),
                                 'creation_date' : m['creation_date'],
                                 'create_by':m['create_by']
                                  }
                    self.otptModCfgin.execute(conn, configObj, tran)
                    tran.commit()
                    tran = None
                except exceptions.IntegrityError, ex:
                    #Another job inserted it just 1/100000 second earlier than
                    #you!!  YG 11/17/2010
                    if str(ex).find("ORA-00001") != -1 or str(ex).lower().find("duplicate") !=-1:
                        if str(ex).find("TUC_OMC_1") != -1:
                            #the config is already in db, get the ID later
                            pass
                        else:
                            #reinsert it if one or two or three of the three attributes (vresion, hash and app) are inserted
                            #just 1/100000 second eailer.
                            try:
                                self.otptModCfgin.execute(conn, configObj, tran)
                                tran.commit()
                                tran = None
                            except exceptions, ex2:
                                if tran:tran.rollback()
                                if conn:conn.close()
                                raise ex2
                except exceptions, ex3:
                    if tran:tran.rollback()
                    if conn:conn.close()
                    raise ex3
                cfgid = self.otptModCfgid.execute(conn,
                                    app = m["app_name"],
                                    release_version = m["release_version"],
                                    pset_hash = m["pset_hash"],
                                    output_label = m["output_module_label"],
                                    global_tag=m['global_tag'])
                otptIdList.append(cfgid)
                key = (m['app_name'] + ':' + m['release_version'] + ':' +
                       m['pset_hash'] + ':' +m['output_module_label'] + ':' +
                       m['global_tag'])
                self.datasetCache['conf'][key] = cfgid
        finally:
            if tran:tran.rollback()
            if conn:conn.close()
        return otptIdList

    def insertDataset(self, blockcontent, otptIdList, migration=False):
        """
        This method insert a datsset from a block object into dbs.
        """
        dataset = blockcontent['dataset']
        conn = self.dbi.connection()

        # First, check and see if the dataset exists.
        try:
            datasetID = self.datasetid.execute(conn, dataset['dataset'])
            dataset['dataset_id'] = datasetID
        except KeyError, ex:
            if conn:conn.close()
            dbsExceptionHandler("dbsException-invalid-input2", "DBSBlockInsert/InsertDataset: Dataset is required.\
                Exception: %s.  troubled dataset are: %s" %(ex.args[0], dataset) )
        except exceptions, ex1:
            if conn:conn.close()
            raise ex1
        if datasetID > 0:
            # Then we already have a valid dataset. We only need to fill the map (dataset & output module config)
            # Skip to the END
            try:
                self.insertDatasetWOannex(dataset = dataset,
                                        blockcontent = blockcontent,
                                        otptIdList = otptIdList, conn = conn,
                                        insertDataset = False, migration=migration)
            finally:
                if conn:conn.close()
            return datasetID

        # Else, we need to do the work
        #Start a new transaction
        tran = conn.begin()
        try:
            #1. Deal with primary dataset. Most primary datasets are
            #pre-installed in db
            primds = blockcontent["primds"]

            primds["primary_ds_id"] = self.primdsid.execute(conn,
                                      primds["primary_ds_name"],transaction=tran)
            if primds["primary_ds_id"] <= 0:
                #primary dataset is not in db yet.
                try:
                    primds["primary_ds_id"] = self.sm.increment(conn, "SEQ_PDS")
                    primds["creation_date"] = primds.get("creation_date", dbsUtils().getTime())
                    #primds["create_by"] = primds.get("create_by", dbsUtils().getCreateBy())
		    if not migration:	
			primds["create_by"] = dbsUtils().getCreateBy()
                    self.primdsin.execute(conn, primds, tran)
                except exceptions.IntegrityError, ex:
                    if (str(ex).find("ORA-00001") != -1 and str(ex).find("TUC_PDS_PRIMARY_DS_NAME") != -1)\
                        or str(ex).lower().find("duplicate") !=-1:
                        primds["primary_ds_id"] = self.primdsid.execute(conn,
                                                primds["primary_ds_name"],
                                                transaction=tran)
                    else:
                        if tran:tran.rollback()
                        if conn:conn.close()
                        raise
                except Exception, ex:
                    if tran:tran.rollback()
                    if conn:conn.close()
                    raise
            dataset['primary_ds_id'] = primds["primary_ds_id"]
            #2 Deal with processed ds
            #processed ds is handled inside of dataset insertion, However we need to make sure it is formated correctly.
            #processed_ds_name is not required pre-exist in the db. will insert with the dataset if not in yet
            #
            #         processed_ds_name=acquisition_era_name[-filter_name][-processing_str]-vprocessing_version
            #         Note [-filterName] is new as 4/30/2012. See ticket #3655. YG
            #
            #althrough acquisition era and processing version is not required for a dataset
            #in the schema(the schema is build this way because
            #we need to accomdate the DBS2 data), but we impose the requirement on the API.
            #So both acquisition and processing eras are required.
            #We do the format checking after we deal with acquision era and processing era.
            #
            #YG 12/07/2011  TK-362

            #3 Deal with Acquisition era
            #import pdb
            #pdb.set_trace()
            aq = {}
            if blockcontent.has_key('acquisition_era'):
                aq = blockcontent['acquisition_era']
            else:
                if conn:conn.close()
                dbsExceptionHandler("dbsException-invalid-input2", "BlockInsert: Acquisition Era is required")
            #is there acquisition?
            if aq.has_key('acquisition_era_name') and aq.has_key('start_date'):
                #for migraction, some of the DBS2 Acquisition does not have start_date, so insert 0.
                if(migration) and  not aq['start_date']:
                    aq['start_date'] = 0
                try:
                    #insert acquisition era into db
                    aq['acquisition_era_id'] = self.sm.increment(conn,"SEQ_AQE")
                    self.acqin.execute(conn, aq, tran)
                    dataset['acquisition_era_id'] = aq['acquisition_era_id']
                except exceptions.IntegrityError, ei:
                    #ORA-01400: cannot insert NULL into required columns, usually it is the NULL on start_date
                    if "ORA-01400" in str(ei) :
                        if tran:tran.rollback()
                        if conn:conn.close()
                        dbsExceptionHandler("dbsException-invalid-input2", "BlockInsert:  acquisition_era_name and start_date are required. NULL was received from user input. Please correct your data.")
                    #ok, already in db?
                    if (str(ei).find("ORA-00001") != -1 and str(ei).find("TUC_AQE_ACQUISITION_ERA_NAME") != -1)\
                        or str(ei).lower().find("duplicate") !=-1:
                        dataset['acquisition_era_id'] = self.acqid.execute(conn, aq['acquisition_era_name'])
                        if dataset['acquisition_era_id'] <= 0:
                            if tran:tran.rollback()
                            if conn:conn.close()
                            dbsExceptionHandler("dbsException-invalid-input2", "BlockInsert: Check the spelling of acquisition Era name.\
                                            the db may already have the same acquisition era, but with different casees.")
                except Exception, ex:
                    if tran:tran.rollback()
                    if conn:conn.close()
                    raise
            else:
                if tran:tran.rollback()
                if conn:conn.close()
                dbsExceptionHandler("dbsException-invalid-input2", "BlockInsert: Acquisition Era is required")

            #4 Deal with Processing era
            pera = {}
            if (blockcontent.has_key('processing_era')):
                pera = blockcontent['processing_era']
            else:
                if tran:tran.rollback()
                if conn:conn.close()
                dbsExceptionHandler('dbsException-invalid-input2', 'BlockInsert:processing version is required')
            #is there processing era?
            if pera.has_key('processing_version'):
                try:
                    #insert processing era into db
                    pera['processing_era_id'] = self.sm.increment(conn,"SEQ_PE")
                    #pera['processing_version'] = pera['processing_version'].upper()
                    self.procsingin.execute(conn, pera, tran)
                    dataset['processing_era_id'] = pera['processing_era_id']
                except exceptions.IntegrityError, ex:
                    if (str(ex).find("ORA-00001: unique constraint") != -1 and str(ex).find("TUC_PE_PROCESSING_VERSION") != -1)\
                        or str(ex).lower().find("duplicate") !=-1:
                        #ok, already in db
                        dataset['processing_era_id'] = self.procsingid.execute(conn, pera['processing_version'])
                    else:
                        if tran:tran.rollback()
                        if conn:conn.close()
                        raise
                except Exception, ex:
                    if tran:tran.rollback()
                    if conn:conn.close()
                    raise
            else:
                if tran:tran.rollback()
                if conn:conn.close()
                dbsExceptionHandler('dbsException-invalid-input2', 'BlockInsert:processing version is required')
            #Make sure processed_ds_name is right format.
            #processed_ds_name=acquisition_era_name[-filter_name][-processing_str]-vprocessing_version
            #In order to accomdate DBS2 data for migration, we turn off this check in migration.
            #These will not cause any problem to none DBS2 datat because when we migration, the none DBS2 data is
            #already checked when they were inserted into the source dbs.  YG 7/12/2012
            if migration:
                pass
            else:
                erals=dataset["processed_ds_name"].rsplit('-')
                if erals[0] != aq["acquisition_era_name"] or erals[len(erals)-1] != "%s%s"%("v",pera["processing_version"]):
                    if tran:tran.rollback()
                    if conn:conn.close()
                    dbsExceptionHandler('dbsException-invalid-input2', "BlockInsert:\
                        processed_ds_name=acquisition_era_name[-filter_name][-processing_str]-vprocessing_version must be satisified.")

            #So far so good, let's committe first 4 db acativties before going on.
            tran.commit()
        except:
            if tran:tran.rollback()
            if conn:conn.close()
            raise

        #Continue for the rest.
        #import pdb
        #pdb.set_trace()
        tran = conn.begin()
        try:
            #5 Deal with physics gruop
            phg = dataset['physics_group_name']
            if phg:
                #Yes, the dataset has physica group.
                phgId = self.phygrpid.execute(conn, phg, transaction=tran)
                if phgId <= 0 :
                    #not in db yet, insert it
                    phgId = self.sm.increment(conn, "SEQ_PG")
                    phygrp = {'physics_group_id':phgId, 'physics_group_name':phg}
                    try:
                        self.phygrpin.execute(conn, phygrp, tran)
                    except exceptions.IntegrityError, ex:
                        if str(ex).find("ORA-00001") != -1 and str(ex).find("PK_PG") != -1:
                            if tran:tran.rollback()
                            if conn:conn.close()
                            dbsExceptionHandler(message='InsertPhysicsGroup Error', logger=self.logger.exception, serverError="InsertPhysicsGroup: "+ str(ex))
                        if (str(ex).find("ORA-00001") != -1 and str(ex).find("TUC_PG_PHYSICS_GROUP_NAME") != -1) or\
                            str(ex).lower().find("duplicate") != -1:
                            phgId = self.phygrpid.execute(conn, phg,transaction=tran)
                            if phgId <= 0:
                                if tran:tran.rollback()
                                if conn:conn.close()
                                dbsExceptionHandler(message='InsertPhysicsGroup Error ', logger=self.logger.exception, serverError="InsertPhysicsGroup: "+str(ex))
                        else:
                            if tran:tran.rollback()
                            if conn:conn.close()
                            raise
                    except Exception, ex:
                        if tran:tran.rollback()
                        if conn:conn.close()
                        raise
                dataset['physics_group_id'] = phgId
                #self.logger.debug("***PHYSICS_GROUP_ID=%s***" %phgId)
            else:
                #no physics gruop for the dataset.
                dataset['physics_group_id'] = None
            del dataset['physics_group_name']
            #6 Deal with Data tier. A dataset must has a data tier
            dataset['data_tier_name'] = dataset['data_tier_name'].upper()
            #handle Tier inside dataset insert.
            #7 Deal with dataset access type. A dataset must have a data type
            dataset['dataset_access_type'] = dataset['dataset_access_type'].upper()
            #handle dataset access type inside dataset insertion with Inser2.
            tran.commit()
        except Exception, ex:
            if tran:tran.rollback()
            if conn:conn.close()
            raise
        try:
            #self.logger.debug("*** Trying to insert the dataset***")
            dataset['dataset_id'] = self.insertDatasetWOannex(dataset = dataset,
                                           blockcontent = blockcontent,
                                           otptIdList = otptIdList,
                                           conn = conn, insertDataset = True, migration=migration)
        finally:
            if tran:tran.rollback()
            if conn:conn.close()

        return dataset['dataset_id']



    def insertDatasetWOannex(self, dataset, blockcontent, otptIdList, conn,
                             insertDataset = True, migration = False):
        """
        _insertDatasetOnly_

        Insert the dataset and only the dataset
        Meant to be called after everything else is put into place.

        The insertDataset flag is set to false if the dataset already exists
        """

        tran = conn.begin()
        try:
            #8 Finally, we have everything to insert a dataset
            if insertDataset:
                # Then we have to get a new dataset ID
                dataset['dataset_id'] = self.datasetid.execute(conn,
                                                dataset['dataset'])
                if dataset['dataset_id'] <= 0:
                    dataset['dataset_id'] = self.sm.increment(conn,"SEQ_DS")
                    dataset['xtcrosssection'] = dataset.get('xtcrosssection', None)
		    if not migration:
			dataset['last_modified_by'] = dbsUtils().getCreateBy()
			dataset['create_by'] = dbsUtils().getCreateBy()
			dataset['creation_date'] = dataset.get('creation_date', dbsUtils().getTime())
			dataset['last_modification_date'] = dataset.get('last_modification_date', dbsUtils().getTime())
                    dataset['xtcrosssection'] = dataset.get('xtcrosssection', None)
                    dataset['prep_id'] = dataset.get('prep_id', None)
                    try:
                        self.datasetin.execute(conn, dataset, tran)
                    except exceptions.IntegrityError, ei:
                        if (str(ei).find("ORA-00001") != -1 and str(ei).find("TUC_DS_DATASET") != -1) or\
                            str(ei).lower().find("duplicate") !=-1:
                            dataset['dataset_id'] = self.datasetid.execute(conn,dataset['dataset'])
                            if dataset['dataset_id'] <= 0:
                                if tran:tran.rollback()
                                if conn:conn.close()
                                dbsExceptionHandler(message='InsertDataset Error', logger=self.logger, serverError="InsertDataset: " + str(ex))
                        else:
                            if tran:tran.rollback()
                            if conn:conn.close()
                            raise
                        #
                    except Exception, ex:
                        #should catch all above exception to rollback. YG Jan 17, 2013
                        if tran:tran.rollback()
                        if conn:conn.close()
                        raise

            #9 Fill Dataset Parentage
            #All parentage are deduced from file parenttage.

            #10 Before we commit, make dataset and output module configuration
            #mapping.  We have to try to fill the map even if dataset is
            #already in dest db
            for c in otptIdList:
                try:
                    dcObj = {
                             'dataset_id' : dataset['dataset_id'],
                             'output_mod_config_id' : c }
                    self.dcin.execute(conn, dcObj, tran)
                except exceptions.IntegrityError, ei:
                    #FIXME YG 01/17/2013
                    if (str(ei).find("ORA-00001") != -1 and str(ei).find("TUC_DC_1") != -1) or \
                            str(ei).lower().find("duplicate")!=-1:
                    #ok, already in db
                    #FIXME: What happens when there are partially in db?
                    #YG 11/17/2010
                        pass
                    else:
                        if tran:tran.rollback()
                        if conn:conn.close()
                        raise
                except Exception, ex:
                    if tran:tran.rollback()
                    if conn:conn.close()
                    raise
            #Now commit everything.
            tran.commit()
        except exceptions.IntegrityError:
            # Then is it already in the database?
            # Not really. We have to check it again. YG Jan 17, 2013
            #FIXME
            pass
        except Exception, ex:
            if tran:tran.rollback()
            if conn:conn.close()
            raise
        finally:
            if tran:tran.rollback()
            if conn:conn.close()
        return dataset['dataset_id']
