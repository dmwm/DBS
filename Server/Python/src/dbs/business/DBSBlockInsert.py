#!/usr/bin/env python
"""
DBS  block insertion for WMAgent
"""
__revision__ = "$Id:$"
__version__ = "$Revision: $"

import threading
import logging
import traceback
import os
import time

from sqlalchemy import exceptions

from WMCore.WorkerThreads.BaseWorkerThread import BaseWorkerThread
from WMCore.DAOFactory import DAOFactory

import json, cjson
import urllib, urllib2

from dbs.utils.dbsUtils import dbsUtils

class DBSBlockInsert :

    """
    Insert a Block and everything under this block. 
    """
    
    def __init__(self, logger, dbi, owner):
        daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        # Setup the DAO objects
        self.primdslist            = daofactory(classname="PrimaryDataset.List")
        self.primdsid       = daofactory(classname="PrimaryDataset.GetID")
        self.datasetlist    = daofactory(classname="Dataset.List")
        self.datasetid      = daofactory(classname="Dataset.GetID")
        self.blocklist            = daofactory(classname="Block.List")
        self.blockid        = daofactory(classname="Block.GetID")
        self.filelist            = daofactory(classname="File.List")
        self.fileid       = daofactory(classname="File.GetID")
        self.filetypeid     = daofactory(classname="FileType.GetID")
        self.fplist            = daofactory(classname="FileParent.List")
        self.fllist            = daofactory(classname="FileLumi.List")
        self.primdstpid            = daofactory(classname='PrimaryDSType.GetID')
        self.tierid            = daofactory(classname='DataTier.GetID')
        self.datatypeid            = daofactory(classname='DatasetType.GetID')
        self.phygrpid            = daofactory(classname='PhysicsGroup.GetID')
        self.acqid          = daofactory(classname='AcquisitionEra.GetID')
        self.procsingid     = daofactory(classname='ProcessingEra.GetID')
        self.procdsid            = daofactory(classname='ProcessedDataset.GetID')
        self.otptModCfgid   = daofactory(classname='OutputModuleConfig.GetIDForBlockInsert')
        self.releaseVid     = daofactory(classname='ReleaseVersion.GetID')
        self.psetHashid     = daofactory(classname='ParameterSetHashe.GetID') 
        self.appid          = daofactory(classname='ApplicationExecutable.GetID') 

        self.procsingin     = daofactory(classname='ProcessingEra.Insert')
        self.acqin          = daofactory(classname='AcquisitionEra.Insert')
        self.procdsin            = daofactory(classname='ProcessedDataset.Insert')
        self.primdsin            = daofactory(classname="PrimaryDataset.Insert")
        self.primdstpin     = daofactory(classname="PrimaryDSType.Insert")
        self.datasetin            = daofactory(classname='Dataset.Insert')
        self.dsparentin     = daofactory(classname='DatasetParent.Insert')
        self.datatypein     = daofactory(classname='DatasetType.Insert')
        self.blockin        = daofactory(classname='Block.Insert')
        self.sm             = daofactory(classname = "SequenceManager")
        self.filein         = daofactory(classname = "File.Insert")
        self.filetypein     = daofactory(classname = "FileType.Insert")
        self.flumiin        = daofactory(classname = "FileLumi.Insert")
        self.fparentin      = daofactory(classname = "FileParent.Insert")
        self.fpbdlist       = daofactory(classname = "FileParentBlock.List")
        self.blkparentin    = daofactory(classname = "BlockParent.Insert")
        self.dsparentin     = daofactory(classname = "DatasetParent.Insert")
        self.blkstats       = daofactory(classname = "Block.ListStats")
        self.blkstatsin     = daofactory(classname = "Block.UpdateStats")
        self.fconfigin      = daofactory(classname='FileOutputMod_config.Insert')
        self.bufdeletefiles = daofactory(classname="FileBuffer.DeleteFiles")
        self.buflist        = daofactory(classname="FileBuffer.List")
        self.buflistblks    = daofactory(classname="FileBuffer.ListBlocks")
        self.buffinddups    = daofactory(classname="FileBuffer.FindDuplicates")
        self.bufdeletedups  = daofactory(classname="FileBuffer.DeleteDuplicates")
        self.compstatusin   = daofactory(classname="ComponentStatus.Insert")
        self.compstatusup   = daofactory(classname="ComponentStatus.Update")
        self.otptModCfgin   = daofactory(classname='OutputModuleConfig.Insert')
        self.releaseVin     = daofactory(classname='ReleaseVersion.Insert')
        self.psetHashin     = daofactory(classname='ParameterSetHashe.Insert')
        self.appin          = daofactory(classname='ApplicationExecutable.Insert')
        self.dcin           = daofactory(classname='DatasetOutputMod_config.Insert')
        self.phygrpin       = daofactory(classname='PhysicsGroup.Insert')
        self.newBlock = False

        # Add a cache that Anzar thinks he needs
        self.datasetCache = {'conf': {}}

    def putBlock(self, blockcontent):
        """
        Insert the data in sereral steps and commit when each step finishes or rollback if there is a problem.
        """
        #YG
        try:
            #1 insert configuration
            print "insert configuration"
            configList = self.insertOutputModuleConfig( blockcontent['dataset_conf_list'])
            #2 insert dataset
            #print "insert dataset"
            datasetId = self.insertDataset( blockcontent, configList)
            #3 Insert Block. 
            #print "insert Block"
            blockId = self.insertBlock( blockcontent, datasetId)
            #4 inser files. If the block is already in db, then we stop inserting the file
            if self.newBlock:
                #print "insert files"
                self.insertFile( blockcontent,blockId,datasetId)
        except Exception, ex:
            raise
    
    def insertFile(self, blockcontent, blockId, datasetId):
        conn = self.dbi.connection()
        fileLumiList = []
        fileTypeObjs = []
        fileConfObjs = []
        logicalFileName ={}
        fileList = blockcontent['files']
        fileConfigList = blockcontent['file_conf_list']
        fileParentList = blockcontent['file_parent_list']
        if not fileList:
            return
        intval = 40
        intvalum = 1000
        intvalfileparent = 120
        intvalfileconf = 1

	donelumi=0
	
        try:
            for i in range(len(fileList)):
                if(i%intval==0):
                    id = self.sm.increment(conn,"SEQ_FL", False, intval)
                fileList[i]['file_id'] = id
                logicalFileName[fileList[i]['logical_file_name']] = id
                fileList[i]['block_id'] = blockId
                fileList[i]['dataset_id'] = datasetId
                #get file type id
                fType = fileList[i]['file_type']
                fTypeid = self.filetypeid.execute(conn, fType)
                if fTypeid <=0 :
                    fTypeid =  self.sm.increment(conn,"SEQ_FT")
                    ftypeO = {'file_type':fType, 'file_type_id':fTypeid}
                    fileTypeObjs.append(ftypeO)
                fileList[i]['file_type_id'] = fTypeid
                del fileList[i]['file_type']
                #get lumi info
                lumi = fileList[i]['file_lumi_list']
                nlumi = len(lumi)
                for j in range(nlumi):
                    lumi[j]['file_id'] = id
		    if (donelumi%intvalum==0):
                        idlumi = self.sm.increment(conn,"SEQ_FLM", False, intvalum)
		    donelumi += 1
                    lumi[j]['file_lumi_id'] = idlumi
                    idlumi += 1;
                fileLumiList[len(fileLumiList):] = lumi
                #remove the lumi list from the file 
                del fileList[i]['file_lumi_list']
                id += 1
        except Exception, ex:
            self.logger.exception("DBS file and lumi section exception: %s" %ex)
            raise
        try:        
            #deal with file parentage
            nfileparent = len(fileParentList)
            for k in range(nfileparent):
                if(k%intvalfileparent==0):
                    idfp = self.sm.increment(conn,"SEQ_FP", False, intvalfileparent)
                fileParentList[k]['file_parent_id'] = idfp 
                idfp += 1
                fileParentList[k]['this_file_id'] = logicalFileName[fileParentList[k]['logical_file_name']]
                parentId = self.fileid.execute(conn, fileParentList[k]['parent_logical_file_name'])
                if parentId <= 0 :
                    raise Exception("File parent %s cannot be found" %fileParentList[k]['parent_logical_file_name'])
                fileParentList[k]['parent_file_id'] = parentId
                del fileParentList[k]['parent_logical_file_name']
                del fileParentList[k]['logical_file_name']
            #deal with file config
            for fc in fileConfigList:
                key=fc['app_name']+':'+fc['release_version']+':'+fc['pset_hash']+':'+fc['output_module_label']
                if not key in (self.datasetCache['conf']).keys():
                    #we expect the config is inserted when the dataset is in.
                    raise Exception("Configuration application name, release version and pset hash: %s, %s ,%s not found" \
                                %(fc['app_name'], fc['release_version'], fc['pset_hash']))
                fcObj={'file_output_config_id':self.sm.increment(conn,"SEQ_FC"), 'file_id':logicalFileName[fc['lfn']]
                       , 'output_mod_config_id': self.datasetCache['conf'][key] }
                fileConfObjs.append(fcObj)
        except Exception, ex:
            self.logger.exception("DBS file parentage and config exception: %s" %ex)
            raise
        try:
            #now we build everything to insert the files.
            tran = conn.begin()
            #insert the file type
            if fileTypeObjs:
                self.filetypein.execute(conn, fileTypeObjs, tran)
            #insert files
            if fileList:
                self.filein.execute(conn, fileList, tran)
            #insert file parents
            if fileParentList:
                self.fparentin.execute(conn, fileParentList, tran)
            #insert file lumi
            if fileLumiList:
		lumiIDs = [l['file_lumi_id'] for l in fileLumiList]
                self.flumiin.execute(conn, fileLumiList, tran)
            #insert file configration
            if fileConfObjs:
                self.fconfigin.execute(conn, fileConfObjs, tran)
            tran.commit()
        except Exception, ex:
            self.logger.exception("DBS file inseration exception: %s" %ex)
            tran.rollback()
            raise
        finally:
            conn.close()


    def insertBlock(self, blockcontent, datasetId):
        """
        Block is very simple to insert

        """

        block = blockcontent['block']
        self.newBlock = False
        #Insert the block
        #import pdb
        #pdb.set_trace()
        try:
            conn = self.dbi.connection()
            tran = conn.begin()
            block['block_id'] = self.sm.increment(conn,"SEQ_BK",)
            block['dataset_id'] =  datasetId
            self.blockin.execute(conn, block, tran)
            self.newBlock = True
        except exceptions.IntegrityError:
            #not sure what happends to WMAgent: Does it try to insert a block again? YG 10/05/2010
            #ok, already in db. We should stop the migration of this block now. Should not try to insert the files.
            #Because we assuem that every migration should migrate all the files.
            block['block_id'] = self.blockid.execute(conn, block['block_name'], transaction=tran)
            tran.rollback()
            return block['block_id']
        except Exception, ex:
            self.logger.exception("DBS block inseration exception: %s" %ex)
            tran.rollback()
            raise
        #Now handle Block Parenttage
        bpList = blockcontent['block_parent_list']
        intval = 10
        if self.newBlock:
            try:
                for i in range(len(bpList)):
                    if(i%intval==0):
                        id = self.sm.increment(conn,"SEQ_BP", False, intval)
                    bpList[i]['block_parent_id'] = id
                    id += 1
                    bpList[i]['this_block_id'] = block['block_id']
                    bpList[i]['parent_block_id'] = self.blockid.execute(conn, bpList[i]['block_name'])
                    if bpList[i]['parent_block_id'] <= 0:
                        if tran:
                            tran.rollback()
                        raise Exception("Parent block: %s not found in db" %bpList[i]['block_name'])
                    del bpList[i]['block_name']
                if bpList and self.newBlock:
                    self.blkparentin.execute(conn, bpList, tran)
            except Exception, ex:
                self.logger.exception("DBS block parentage inseration exception: %s" %ex)
                tran.rollback()
                raise
        #Ok, we can commit everything.
        try:
            tran.commit()
        except:
            if tran:
                tran.rollback()
            raise
        finally:
            conn.close()
        return block['block_id']

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
                                                  output_label = c["output_module_label"])
                if cfgid <=0 :
                    missingList.append(c)
                else:
                    key = c['app_name']+':'+c['release_version']+':'+c['pset_hash']+':'+c['output_module_label']
                    self.datasetCache['conf'][key] = cfgid
                    print "About to set cfgid: %s" % str(cfgid)
        except Exception, ex:
            self.logger.exception("DBS output module config  exception: %s" %ex)
            raise
        #Now insert the missing configs
        try:
            #tran = conn.begin()
            for m in missingList:
                # Start a new transaction
                # This is to see if we can get better results
                # by committing early if we're submitting
                # multiple blocks with similar features
                tran = conn.begin()
                
                #find release version id
                reId = self.releaseVid.execute(conn, m["release_version"])
                if reId <= 0:
                    #not found release version in db, insert it now
                    reId = self.sm.increment(conn, "SEQ_RV")
                    reobj={"release_version": m["release_version"], "release_version_id": reId}
                    self.releaseVin.execute(conn, reobj, tran)
                #find pset hash id
                pHId = self.psetHashid.execute(conn, m["pset_hash"], transaction=tran)
                if pHId <= 0:
                    #not found p set hash in db, insert it now
                    pHId = self.sm.increment(conn, "SEQ_PSH")
                    pHobj={"pset_hash": m["pset_hash"], "parameter_set_hash_id": pHId, 'name':m.get('name', '')}
                    self.psetHashin.execute(conn, pHobj, tran)
                #find application id
                appId = self.appid.execute(conn, m["app_name"])
                if appId <= 0:
                    #not found application in db, insert it now
                    appId = self.sm.increment(conn, "SEQ_AE")
                    appobj={"app_name": m["app_name"], "app_exec_id": appId}
                    self.appin.execute(conn, appobj, tran)
                    # TODO: WHY THE F*%$ is this necessary?
                    appId = self.appid.execute(conn, m["app_name"])
                #Now insert the config
                
                # Sort out the mess
                # We're having some problems with different threads
                # committing different pieces at the same time
                # This makes the output module config ID wrong
                # Trying to catch this via exception handling on duplication
                tran.commit()

                # Start a new transaction
                tran = conn.begin()
                try:
                    #get output module config id
                    cfgid = self.sm.increment(conn, "SEQ_OMC")
                    configObj = {'output_mod_config_id':cfgid , 'app_exec_id':appId,  'release_version_id':reId,  \
                                 'parameter_set_hash_id':pHId , 'output_module_label':m['output_module_label'],   \
                                 'creation_date':None, 'create_by':''}
                    self.otptModCfgin.execute(conn, configObj, tran)
                except exceptions.IntegrityError:
                    # Already in dataset.
                    # How did that happen?
                    cfgid = self.otptModCfgid.execute(conn, app = m["app_name"],
                                                  release_version = m["release_version"],
                                                  pset_hash = m["pset_hash"],
                                                  output_label = m["output_module_label"])
                # End the transaction
                tran.commit()
                    
                    
                otptIdList.append(cfgid)
                key = m['app_name']+':'+m['release_version']+':'+m['pset_hash']+':'+m['output_module_label']
                self.datasetCache['conf'][key] = cfgid

                
            #tran.commit()
            #Duplicated entries? No, unless another thread insert it ( very small possiblity). It should not because this is the missing list.
            #If this happends, I'd rather it raises exception.
        except Exception, ex:
            self.logger.exception("DBS output module configure insertion exception: %s" %ex)
            tran.rollback()
            raise
        finally:
            conn.close()
        return otptIdList


    def insertDataset(self, blockcontent, otptIdList):
        """
        This method insert a datsset from a block object into dest dbs. When data is not completed in
        the block object. It will reterive data from the source url. 
        """
        #import pdb
        #pdb.set_trace()
        dataset = blockcontent['dataset']
        conn = self.dbi.connection()

        # First, check and see if the dataset exists.
        datasetID = self.datasetid.execute(conn, dataset['dataset'])
        dataset['dataset_id'] = datasetID
        if datasetID > -1:
            # Then we already have a valid dataset.
            # Skip to the END
            try:
                self.insertDatasetOnly(dataset = dataset, blockcontent = blockcontent,
                                       otptIdList = otptIdList, conn = conn,
                                       insertDataset = False)
            except Exception:
                raise
            finally:
                conn.close()
                
            return datasetID

        # Else, we need to do the work


        
        try:
            #Start a new transaction 
            tran = conn.begin()
            
            #1. Deal with primary dataset. Most primary datasets are perinstalled in db  
            primds = blockcontent["primds"]
            primds["primary_ds_id"] = self.primdsid.execute(conn, primds["primary_ds_name"], transaction=tran)
            if primds["primary_ds_id"] <= 0:
                #primary dataset is not in db yet. We need to check if the primary_ds_type before insert primary ds
                primds["primary_ds_type_id"] = self.primdstpid.execute(conn, primds["primary_ds_type"], transaction=tran)
                if primds["primary_ds_type_id"] <= 0:
                    #primary ds type is not in db yet. Insert it now
                    primds["primary_ds_type_id"] = self.sm.increment(conn,"SEQ_PDT")
                    obj={'primary_ds_type_id':primds["primary_ds_type_id"], 'primary_ds_type':primds["primary_ds_type"]}
                    self.primdstpin.execute(conn, obj, tran)
                #Now inserting primary ds. Clean up dao object befer inserting
                del primds["primary_ds_type"]
                primds["primary_ds_id"] = self.sm.increment(conn, "SEQ_PDS")
                try:
                    self.primdsin.execute(conn, primds, tran)
                except exceptions.IntegrityError:
                    # Already in the database?
                    # do Nothing
                    pass
            dataset['primary_ds_id'] = primds["primary_ds_id"]
            #2 Deal with processed ds
            try:
                #Let's insert the processed ds since it is not pre-inserted at schema level
                daoproc={'processed_ds_id':self.sm.increment(conn,"SEQ_PSDS"), 
                                "processed_ds_name": dataset['processed_ds_name']}
                self.procdsin.execute(conn,daoproc, tran)
                dataset['processed_ds_id'] = daoproc['processed_ds_id']
            except exceptions.IntegrityError:
                #Ok, it is in db already. Get the ID
                dataset['processed_ds_id'] = self.procdsid.execute(conn, dataset['processed_ds_name'])
            except Exception, ex:
                self.logger.exception("DBS processed ds inseration exception: %s" %ex)
                tran.rollback
                raise
            #
            del dataset["processed_ds_name"]
            #3 Deal with Acquisition era
            aq={}
            if blockcontent.has_key('acquisition_era'):
                aq = blockcontent['acquisition_era']
            #is there acquisition?
            if aq:
                #check if acquisition in cache
                try:
                    #insert acquisition era into db
                    aq['acquisition_era_id'] = self.sm.increment(conn,"SEQ_AQE")
                    self.acqin.execute(conn, aq, tran)
                    dataset['acquisition_era_id'] = aq['acquisition_era_id']
                except exceptions.IntegrityError:
                    #ok, already in db
                    dataset['acquisition_era_id'] = self.acqid.execute(conn, aq['acquisition_era_name'])
                except Exception, ex:
                    self.logger.exception("DBS acquisition era inseration exception: %s" %ex)
                    tran.rollback
                    raise
            else:
                #no acquisition era for this dataset
                pass
            #4 Deal with Processing era
            pera={}
            if (blockcontent.has_key('processing_era')):
                pera = blockcontent['processing_era']
            #is there processing era?
            if pera:
                try:
                    #insert processing era into db
                    pera['processing_era_id'] = self.sm.increment(conn,"SEQ_PE")
                    self.procsingin.execute(conn, pera, tran)
                    dataset['processing_era_id'] = pera['processing_era_id']
                except exceptions.IntegrityError:
                    #ok, already in db
                    dataset['processing_era_id'] = self.procsingid.execute(conn, pera['processing_version'])
                except Exception, ex:
                    self.logger.exception("DBS processing era inseration exception: %s" %ex)
                    tran.rollback()
                    raise
            else:
                #no processing era for this dataset
                pass
            #let's committe first 4 db acativties before going on.
            tran.commit()
        except:
            if tran:
                tran.rollback()
            raise
        
        #Continue for the rest.
        try:
            tran=conn.begin()
            #5 Deal with physics gruop
            phg = dataset['physics_group_name']
            if phg:
                #Yes, the dataset has physica group. 
                #find in db since not find it in cache
                phgId = self.phygrpid.execute(conn, phg, transaction=tran)
                if phgId <=0 :
                    #not in db yet, insert it
                    phygrp={'physics_group_id':self.sm.increment(conn,"SEQ_PG"), 'physics_group_name':phg}
                    try:
                        self.phygrpin.execute(conn, phygrp, tran)
                    except exceptions.IntegrityError:
                        # This seems to happen and I don't know why
                        # It only occurs when you have a new group
                        # But it's still a killer bug
                        pass
                    
                dataset['physics_group_id'] = phgId
            else:
                #no physics gruop for the dataset.
                pass
            del dataset['physics_group_name']
            #6 Deal with Data tier. A dataset must has a data tier
            dataT = dataset['data_tier_name']
            dataTId = self.tierid.execute(conn, dataT, transaction=tran)
            if dataTId <= 0 :
                #not in db. Insert the tier
                dataTId = self.sm.increment(conn,"SEQ_DT")
                theTier={'data_tier_id': dataTId,  'data_tier_name': dataT}                    
                self.tierin.execute(conn, theTier, tran)
            dataset['data_tier_id'] = dataTId
            del dataset['data_tier_name']
            #7 Deal with dataset access type. A dataset must have a data type
            dsTp = dataset['dataset_access_type']
            dsTpId = self.datatypeid.execute(conn, dsTp, transaction=tran)
            if dsTpId <= 0 :
                #not in db. Insert the type
                dsTpId = self.sm.increment(conn,"SEQ_DTP")
                theType={'dataset_access_type':dsTp, 'dataset_access_type_id':dsTpId}
                self.datatypein.execute(conn, theType, tran)
            dataset['dataset_access_type_id'] = dsTpId
            del dataset['dataset_access_type']
            tran.commit()
        except Exception, ex:
            self.logger.exception("DBS pre-dataset insertion exception: %s" %ex)
            tran.rollback()
            raise
        try:
            dataset['dataset_id'] = self.insertDatasetOnly(dataset = dataset,
                                                           blockcontent = blockcontent,
                                                           otptIdList = otptIdList, conn = conn)
        except Exception:
            raise
        finally:
            conn.close()

            
#            tran = conn.begin()
#            #8 Finally, we have everything to insert a dataset
#            datasetID = self.datasetid.execute(conn, dataset['dataset'])
#            self.datasetin.execute(conn, dataset, tran)
#            #9 Fill Dataset Parentage
#            dsPList = blockcontent['ds_parent_list']
#            dsParentObjList=[]
#            for p in dsPList:
#                dsParentObj={'dataset_parent_id': self.sm.increment(conn,"SEQ_DP"), 'this_dataset_id': dataset['dataset_id']\
#                                 , 'parent_dataset_id' : self.datasetid.execute(conn, p['parent_dataset'])}
#                dsParentObjList.append(dsParentObj)
#            #insert dataset parentage in bulk
#            if dsParentObjList:
#                self.dsparentin.execute(conn, dsParentObjList, tran)            
#            #10 Before we commit, make dataset and output module configure mapping. 
#            #We have to try to fill the map even if dataset is already in dest db
#            #import pdb
#            #pdb.set_trace()
#            for c in otptIdList:
#                try:
#                    dcId = self.sm.increment(conn,"SEQ_DC")
#                    dcObj ={'ds_output_mod_conf_id': dcId, \
#                         'dataset_id':dataset['dataset_id'] , 'output_mod_config_id':c }
#                    self.dcin.execute(conn, dcObj, tran)
#                except exceptions.IntegrityError:
#                    #ok, already in db
#                    pass
#                except Exception, ex:
#                    self.logger.exception("DBS dataset and output module mapping inseration exception: %s" %ex)
#                    if tran:
#                        tran.rollback()
#                    raise 
#            tran.commit()
#        except exceptions.IntegrityError:
#            # Then is it already in the database?
#            # Maybe.  See what happens if we ignore
#            pass
#        except Exception, ex:
#            self.logger.exception("DBS dataset inseration exception: %s" %ex)
#            tran.rollback()
#            raise
#        finally:
#            conn.close()
        return dataset['dataset_id']



    def insertDatasetOnly(self, dataset, blockcontent, otptIdList, conn, insertDataset = True):
        """
        _insertDatasetOnly_

        Insert the dataset and only the dataset
        Meant to be called after everything else is put into place.

        The insertDataset flag is set to false if the dataset already exists
        """


        try:
            tran = conn.begin()
            #8 Finally, we have everything to insert a dataset
            print "About to try dataset insertion"
            if insertDataset:
                # Then we have to get a new dataset ID
                datasetID = self.datasetid.execute(conn, dataset['dataset'])
                print "Searched for datasetid: %s" % str(datasetID)
                if datasetID < 0:
                    dataset['dataset_id'] = self.sm.increment(conn,"SEQ_DS")
                else:
                    dataset['dataset_id'] = datasetID
                self.datasetin.execute(conn, dataset, tran)
                
            #9 Fill Dataset Parentage
            dsPList = blockcontent['ds_parent_list']
            dsParentObjList=[]
            for p in dsPList:
                dsParentObj={'dataset_parent_id': self.sm.increment(conn,"SEQ_DP"), 'this_dataset_id': dataset['dataset_id']\
                                 , 'parent_dataset_id' : self.datasetid.execute(conn, p['parent_dataset'])}
                dsParentObjList.append(dsParentObj)
            #insert dataset parentage in bulk
            if dsParentObjList:
                self.dsparentin.execute(conn, dsParentObjList, tran)            
            #10 Before we commit, make dataset and output module configure mapping. 
            #We have to try to fill the map even if dataset is already in dest db
            #import pdb
            #pdb.set_trace()
            for c in otptIdList:
                try:
                    dcId = self.sm.increment(conn,"SEQ_DC")
                    dcObj ={'ds_output_mod_conf_id': dcId, \
                         'dataset_id':dataset['dataset_id'] , 'output_mod_config_id':c }
                    self.dcin.execute(conn, dcObj, tran)
                except exceptions.IntegrityError:
                    #ok, already in db
                    pass
                except Exception, ex:
                    self.logger.exception("DBS dataset and output module mapping inseration exception: %s" %ex)
                    if tran:
                        tran.rollback()
                    raise 
            tran.commit()
        except exceptions.IntegrityError:
            # Then is it already in the database?
            # Maybe.  See what happens if we ignore
            pass
        except Exception, ex:
            self.logger.exception("DBS dataset inseration exception: %s" %ex)
            tran.rollback()
            raise

        return dataset['dataset_id']
