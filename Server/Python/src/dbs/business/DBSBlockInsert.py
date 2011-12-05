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
        self.appid          = daofactory(classname=
                                            "ApplicationExecutable.GetID") 

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

    def putBlock(self, blockcontent):
        """
        Insert the data in sereral steps and commit when each step finishes or rollback if there is a problem.
        """
        #YG
        try:
            #1 insert configuration
            configList = self.insertOutputModuleConfig(
                            blockcontent['dataset_conf_list'])
            #2 insert dataset
            datasetId = self.insertDataset(blockcontent, configList)
            #3 Insert Block. 
            blockId, newBlock = self.insertBlock(blockcontent, datasetId)
            #4 insert files. If the block is already in db, then we stop
            #inserting the file
            if newBlock:
                self.insertFile(blockcontent, blockId, datasetId)
        except Exception, ex:
            raise
    
    def insertFile(self, blockcontent, blockId, datasetId):
        conn = self.dbi.connection()
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
                fileList[i]['last_modification_date'] = fileList[i].get('last_modification_date', dbsUtils().getTime())
                fileList[i]['last_modified_by'] = fileList[i].get('last_modified_by', dbsUtils().getCreateBy())

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
            raise
        try: 
            #deal with file parentage. 
            #At the meantime, we need to build block and dataset parentage.
            #All parentage is deduced from file paretage,
            #10/16/2011
            nfileparent = len(fileParentList)
            bkParentList = []
            dsParentList = []
            #import pdb
            #pdb.set_trace()
            for k in range(nfileparent):
                #import pdb
                #pdb.set_trace()
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
                    dbsExceptionHandler('dbsException-missing-data', 'Required Configuration application name, release version,\
                        pset hash and global tag: %s, %s\
                        ,%s,%s not found in DB' %(fc['app_name'], fc['release_version'], fc['pset_hash'], fc['global_tag']))
                fcObj = {'file_id' : logicalFileName[fc['lfn']],
                         'output_mod_config_id': self.datasetCache['conf'][key]}
                fileConfObjs.append(fcObj)
        except Exception, ex:
            raise
        try:
            #now we build everything to insert the files.
            tran = conn.begin()
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
                    if str(ex).find("ORA-00001") != -1 or str(ex).find("unique constraint") != -1 or str(ex).lower().find("duplicate") != -1:
                        pass
                    elif str(ex).find("ORA-01400") != -1:
                        if tran:tran.rollback()
                        raise
                    else:
                        if tran:tran.rollback()
                        raise
            for k in range(dsk):
                try:
                    self.dsparentin2.execute(conn, dsParentList[k], transaction=tran)
                except exceptions.IntegrityError, ex: 
                    if str(ex).find("ORA-00001") != -1 or str(ex).find("unique constraint") != -1 or str(ex).lower().find("duplicate") != -1: 
                        pass
                    elif str(ex).find("ORA-01400") != -1: 
                        if tran:tran.rollback()
                        raise
                    else:
                        if tran:tran.rollback()
                        raise
            #finally, commit everything for file.
            tran.commit()
        except Exception, ex1:
            if tran:
                tran.rollback()
            raise
        finally:
            if tran:
                tran.close()
            if conn:
                conn.close()

    def insertBlock(self, blockcontent, datasetId):
        """
        Block is very simple to insert

        """

        block = blockcontent['block']
        newBlock = False
        #Insert the block
        #import pdb
        #pdb.set_trace()
        try:
            conn = self.dbi.connection()
            tran = conn.begin()
            block['block_id'] = self.sm.increment(conn, "SEQ_BK",)
            block['dataset_id'] =  datasetId
            self.blockin.execute(conn, block, tran)
            newBlock = True
        except exceptions.IntegrityError:
            #not sure what happends to WMAgent: Does it try to insert a
            #block again? YG 10/05/2010
            #Talked with Matt N: We should stop insertng this block now.
            #This means there is some trouble.
            #Throw exception to let the up layer know. YG 11/17/2010
            tran.rollback()
            dbsExceptionHandler("dbsException-invalid-input","DBSBlockInsert/insertBlock. Block already exists.")
        except Exception, ex:
            tran.rollback()
            raise
        #Now handle Block Parenttage.
        #All Praentage will be deduced from file parentage. 
        #Ok, we can commit everything.
        try:
            tran.commit()
        except:
            if tran:
                tran.rollback()
            raise
        finally:
            if tran:
                tran.close()
            if conn:
                conn.close()
        return block['block_id'], newBlock

    def insertOutputModuleConfig(self, remoteConfig):
        """
        Insert Release version, application, parameter set hashes and the map(output module config).

        """
        otptIdList = []
        missingList = []
        try:
            conn = self.dbi.connection()
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
            raise

        if len(missingList)==0:
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
                    configObj = {"release_version": m["release_version"],
                                 "pset_hash": m["pset_hash"],
                                 "app_name": m["app_name"],                
                                 'output_module_label' : m['output_module_label'],
                                 'global_tag' : m['global_tag'],
                                 'scenario' : m.get('scenario', None),
                                 'creation_date' : m.get('creation_date', dbsUtils().getTime()),
                                 'create_by':m.get('create_by', dbsUtils().getCreateBy())}              
                    self.otptModCfgin.execute(conn, configObj, tran)
                    tran.commit()
                except exceptions.IntegrityError, ex:
                    #Another job inserted it just 1/100000 second earlier than
                    #you!!  YG 11/17/2010
                    if str(ex).find("unique constraint") != -1:
                        if str(ex).find("TUC_OMC_1") != -1: 
                            #the config is already in db, get the ID later
                            pass
                        else:
                            #reinsert it if one or two or three of the three attributes (vresion, hash and app) are inserted 
                            #just 1/100000 second eailer.
                            self.otptModCfgin.execute(conn, configObj, tran)
                            tran.commit()
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
        except Exception, ex:
            if tran:
                tran.rollback()
            raise
        finally:
            if tran:
                tran.close()
            if conn:
                conn.close()
        return otptIdList

    def insertDataset(self, blockcontent, otptIdList):
        """
        This method insert a datsset from a block object into dbs.
        """
        #import pdb
        #pdb.set_trace()
        dataset = blockcontent['dataset']
        conn = self.dbi.connection()

        # First, check and see if the dataset exists.
        datasetID = self.datasetid.execute(conn, dataset['dataset'])
        dataset['dataset_id'] = datasetID
        if datasetID > 0:
            # Then we already have a valid dataset.
            # Skip to the END
            try:
                self.insertDatasetWOannex(dataset = dataset, 
                                        blockcontent = blockcontent,
                                        otptIdList = otptIdList, conn = conn,
                                        insertDataset = False)
            finally:
                if conn:
                    conn.close()
                
            return datasetID

        # Else, we need to do the work


        
        try:
            #Start a new transaction 
            tran = conn.begin()
            
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
                    primds["create_by"] = primds.get("create_by", dbsUtils().getCreateBy())
                    self.primdsin.execute(conn, primds, tran)
                except exceptions.IntegrityError:
                    primds["primary_ds_id"] = self.primdsid.execute(conn,
                                                primds["primary_ds_name"],
                                                transaction=tran)
                except Exception, ex:   
                    tran.rollback()
                    raise
            dataset['primary_ds_id'] = primds["primary_ds_id"]
            #2 Deal with processed ds
            #processed ds is handled inside of dataset insertion
            
            #3 Deal with Acquisition era
            aq = {}
            if blockcontent.has_key('acquisition_era'):
                aq = blockcontent['acquisition_era']
            else: dataset['acquisition_era_id'] = None    
            #is there acquisition?
            if aq:
                try:
                    #insert acquisition era into db
                    aq['acquisition_era_id'] = self.sm.increment(conn,"SEQ_AQE")
                    aq['acquisition_era_name'] = aq['acquisition_era_name'].upper()
                    self.acqin.execute(conn, aq, tran)
                    dataset['acquisition_era_id'] = aq['acquisition_era_id']
                except exceptions.IntegrityError:
                    #ok, already in db
                    dataset['acquisition_era_id'] = self.acqid.execute(conn,
                                            aq['acquisition_era_name'].upper())
                except Exception, ex:
                    tran.rollback()
                    raise
            else:
                #no acquisition era for this dataset
                dataset['acquisition_era_id'] = None
            #4 Deal with Processing era
            pera = {}
            if (blockcontent.has_key('processing_era')):
                pera = blockcontent['processing_era']
            else: dataset['processing_era_id'] = None
            #is there processing era?
            if pera:
                try:
                    #insert processing era into db
                    pera['processing_era_id'] = self.sm.increment(conn,"SEQ_PE")
                    pera['processing_version'] = pera['processing_version'].upper()
                    self.procsingin.execute(conn, pera, tran)
                    dataset['processing_era_id'] = pera['processing_era_id']
                except exceptions.IntegrityError:
                    #ok, already in db
                    dataset['processing_era_id'] = self.procsingid.execute(
                                            conn, pera['processing_version'])
                except Exception, ex:
                    tran.rollback()
                    raise
            else:
                #no processing era for this dataset
                dataset['processing_era_id'] = None
            #let's committe first 4 db acativties before going on.
            tran.commit()
        except:
            if tran:
                tran.rollback()
            raise
        
        #Continue for the rest.
        try:
            tran = conn.begin()
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
                    except exceptions.IntegrityError:
                        phgId = self.phygrpid.execute(conn, phg,
                                                      transaction=tran)
                    except Exception, ex:
                        tran.rollback()
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
            if tran:
                tran.rollback()
            raise
        try:
            #self.logger.debug("*** Trying to insert the dataset***")
            dataset['dataset_id'] = self.insertDatasetWOannex(dataset = dataset,
                                           blockcontent = blockcontent,
                                           otptIdList = otptIdList,
                                           conn = conn)
        finally:
            if tran:
                tran.close()
            if conn:
                conn.close()
            
        return dataset['dataset_id']



    def insertDatasetWOannex(self, dataset, blockcontent, otptIdList, conn,
                             insertDataset = True):
        """
        _insertDatasetOnly_

        Insert the dataset and only the dataset
        Meant to be called after everything else is put into place.

        The insertDataset flag is set to false if the dataset already exists
        """
        #import pdb
        #pdb.set_trace()


        try:
            tran = conn.begin()
            #8 Finally, we have everything to insert a dataset
            if insertDataset:
                # Then we have to get a new dataset ID
                dataset['dataset_id'] = self.datasetid.execute(conn,
                                                dataset['dataset'])
                if dataset['dataset_id'] <= 0:
                    dataset['dataset_id'] = self.sm.increment(conn,"SEQ_DS")
                    dataset['xtcrosssection'] = dataset.get('xtcrosssection', None)
                    dataset['creation_date'] = dataset.get('creation_date', dbsUtils().getTime())
                    dataset['create_by'] = dataset.get('create_by', dbsUtils().getCreateBy())
                    dataset['last_modification_date'] = dataset.get('last_modification_date', dbsUtils().getTime())
                    dataset['last_modified_by'] = dataset.get('last_modified_by', dbsUtils().getCreateBy())
                    dataset['xtcrosssection'] = dataset.get('xtcrosssection', None)
                    dataset['prep_id'] = dataset.get('prep_id', None)
                    try:
                        self.datasetin.execute(conn, dataset, tran)
                    except exceptions.IntegrityError:
                        dataset['dataset_id'] = self.datasetid.execute(conn,
                                                        dataset['dataset'])
                    except Exception, ex:
                        if tran:
                            tran.rollback()
                        if conn:
                            conn.close()
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
                except exceptions.IntegrityError:
                    #ok, already in db
                    #FIXME: What happens when there are partially in db?
                    #YG 11/17/2010
                    pass
                except Exception, ex:
                    if tran:
                        tran.rollback()
                    raise 
            tran.commit()
        except exceptions.IntegrityError:
            # Then is it already in the database?
            # Maybe.  See what happens if we ignore
            pass
        except Exception, ex:
            tran.rollback()
            raise

        return dataset['dataset_id']
