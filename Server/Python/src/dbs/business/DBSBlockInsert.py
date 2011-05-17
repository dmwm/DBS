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
        self.otptModCfgid   = daofactory(classname='OutputModuleConfig.GetID')
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
        #self.newBlock = False

        # Add a cache that Anzar thinks he needs.
        #Cache is only need for output configure module since it is shared between file and dataset.
        #Others go to db in different jobs. YG 11/17/2010
        self.datasetCache = {'conf': {}}

    def putBlock(self, blockcontent):
        """
        Insert the data in sereral steps and commit when each step finishes or rollback if there is a problem.
        """
        #YG
        try:
            #1 insert configuration
            #print "insert configuration"
            configList = self.insertOutputModuleConfig( blockcontent['dataset_conf_list'])
            #2 insert dataset
            #print "insert dataset"
            datasetId = self.insertDataset( blockcontent, configList)
            #3 Insert Block. 
            #print "insert Block"
            blockId, newBlock = self.insertBlock( blockcontent, datasetId)
            #print "Retrieved result for block %s" % str(blockId)
            #4 inser files. If the block is already in db, then we stop inserting the file
            if newBlock:
                #print "About to insert files for block %s" % str(blockId)
                #print "insert files"
                self.insertFile( blockcontent,blockId,datasetId)
        except Exception, ex:
            #self.logger.exception("%s DBSBlockInsert. %s\n." %(DBSEXCEPTIONS['dbsException-2'], ex))
            raise
    
    def insertFile(self, blockcontent, blockId, datasetId):
        #print  blockcontent
        conn = self.dbi.connection()
        fileLumiList = []
        fileTypeObjs = []
        fileConfObjs = []
        logicalFileName ={}
        fileList = blockcontent['files']
        fileConfigList = blockcontent['file_conf_list']
        if blockcontent.has_key('file_parent_list'):
            fileParentList = blockcontent['file_parent_list']
        else:
            fileParentList=[]
        if not fileList:
            return
        intval = 40
        intvalum = 1000
        #intvalfileparent = 120
        intvalfileconf = 1

	donelumi=0
        #import pdb
        #pdb.set_trace()
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
                fileList[i]['last_modification_date'] = fileList[i].get('last_modification_date', None)
                fileList[i]['last_modified_by'] = fileList[i].get('last_modified_by', None)

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
            #self.logger.exception("%s DBSBlockInsert/File. %s\n." %(DBSEXCEPTIONS['dbsException-2'], ex))
            raise
        try:        
            #deal with file parentage
            nfileparent = len(fileParentList)
            for k in range(nfileparent):
                #if(k%intvalfileparent==0):
                    #idfp = self.sm.increment(conn,"SEQ_FP", False, intvalfileparent)
                #fileParentList[k]['file_parent_id'] = idfp 
                #idfp += 1
                fileParentList[k]['this_file_id'] = logicalFileName[fileParentList[k]['logical_file_name']]
                parentId = self.fileid.execute(conn, fileParentList[k]['parent_logical_file_name'])
                if parentId <= 0 :
                    raise Exception("File parent %s cannot be found" %fileParentList[k]['parent_logical_file_name'])
                fileParentList[k]['parent_file_id'] = parentId
                del fileParentList[k]['parent_logical_file_name']
                del fileParentList[k]['logical_file_name']
            #deal with file config
            for fc in fileConfigList:
                key=fc['app_name']+':'+fc['release_version']+':'+fc['pset_hash']+':'\
                    +fc['output_module_label'] +':'+fc['global_tag']
                if not key in (self.datasetCache['conf']).keys():
                    #we expect the config is inserted when the dataset is in.
                    raise Exception("Configuration application name, release version, pset hash and global tag: %s, %s ,%s,%s not found" \
                                %(fc['app_name'], fc['release_version'], fc['pset_hash'], fc['global_tag']))
                fcObj={'file_output_config_id':self.sm.increment(conn,"SEQ_FC"), 'file_id':logicalFileName[fc['lfn']]
                       , 'output_mod_config_id': self.datasetCache['conf'][key] }
                fileConfObjs.append(fcObj)
        except Exception, ex:
            #self.logger.exception("%s DBSBlockInsert/file parentage. %s\n." %(DBSEXCEPTIONS['dbsException-2'], ex))
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
            #self.logger.exception("%s DBSBlockInsert/file insertion. %s\n." %(DBSEXCEPTIONS['dbsException-2'], ex))
            tran.rollback()
            raise
        finally:
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
            block['block_id'] = self.sm.increment(conn,"SEQ_BK",)
            block['dataset_id'] =  datasetId
            self.blockin.execute(conn, block, tran)
            newBlock = True
        except exceptions.IntegrityError:
            #not sure what happends to WMAgent: Does it try to insert a block again? YG 10/05/2010
            #Talked with Matt N: We should stop insertng this block now. This means there is some trouble.
            #Throw exception to let the up layer know. YG 11/17/2010
            #block['block_id'] = self.blockid.execute(conn, block['block_name'], transaction=tran)
            tran.rollback()
            #return block['block_id']
            #self.logger.exception("%s DBSBlockInsert/insertBlock found Duplicated block: %s" %block['block_name'])
            raise
        except Exception, ex:
            #self.logger.exception("%s DBSBlockInsert/insertBlock. %s\n." %(DBSEXCEPTIONS['dbsException-2'], ex))
            tran.rollback()
            raise
        #Now handle Block Parenttage
        if blockcontent.has_key('block_parent_list'):
            bpList = blockcontent['block_parent_list']
            intval = 10
            if newBlock:
                try:
                    for i in range(len(bpList)):
                        #updated due to schema update.
                        bpList[i]['this_block_id'] = block['block_id']
                        bpList[i]['parent_block_id'] = self.blockid.execute(conn, bpList[i]['block_name'])
                        if bpList[i]['parent_block_id'] <= 0:
                            if tran:
                                tran.rollback()
                            raise Exception("Parent block: %s not found in db" %bpList[i]['block_name'])
                        del bpList[i]['block_name']
                    if bpList and newBlock:
                        self.blkparentin.execute(conn, bpList, tran)
                except Exception, ex:
                    #self.logger.exception("%s DBSBlockInsert/block parentage. %s\n." %(DBSEXCEPTIONS['dbsException-2'], ex))
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
        return block['block_id'], newBlock

    def insertOutputModuleConfig(self, remoteConfig):
        """
        Insert Release version, application, parameter set hashes and the map(output module config).

        """
        #import pdb
        #pdb.set_trace()
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
                if cfgid <=0 :
                    missingList.append(c)
                else:
                    key = c['app_name']+':'+c['release_version']+':'+c['pset_hash']+':'\
                          +c['output_module_label'] +':'+c['global_tag']
                    self.datasetCache['conf'][key] = cfgid
                    otptIdList.append(cfgid)
                    #print "About to set cfgid: %s" % str(cfgid)
        except Exception, ex:
            #self.logger.exception("%s DBSBlockInsert/outputModuleConfig. %s\n." %(DBSEXCEPTIONS['dbsException-2'], ex))
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
                #We are trying to commit right after almost every db activity. This may help with mutiple 
                #jobs running with the same configuration, but will slow done DBS. YG 11/17/2010

                #find release version id
                reId = self.releaseVid.execute(conn, m["release_version"])
                if reId <= 0:
                    #not found release version in db, insert it now
                    reId = self.sm.increment(conn, "SEQ_RV")
                    reobj={"release_version": m["release_version"], "release_version_id": reId}
                    #in case another job just commit it 1/10000 second earlier. YG 11/17/2010
                    try:
                        self.releaseVin.execute(conn, reobj, tran)
                    except exceptions.IntegrityError:
                        reId = self.releaseVid.execute(conn, m["release_version"])
                #find pset hash id
                pHId = self.psetHashid.execute(conn, m["pset_hash"], transaction=tran)
                if pHId <= 0:
                    #not found p set hash in db, insert it now
                    pHId = self.sm.increment(conn, "SEQ_PSH")
                    pHobj={"pset_hash": m["pset_hash"], "parameter_set_hash_id": pHId, 'name':m.get('name', '')}
                    try:
                        self.psetHashin.execute(conn, pHobj, tran)
                    except exceptions.IntegrityError:
                        pHId = self.psetHashid.execute(conn, m["pset_hash"], transaction=tran)
                #find application id
                appId = self.appid.execute(conn, m["app_name"])
                if appId <= 0:
                    #not found application in db, insert it now
                    appId = self.sm.increment(conn, "SEQ_AE")
                    appobj={"app_name": m["app_name"], "app_exec_id": appId}
                    try:
                        self.appin.execute(conn, appobj, tran)
                    except exceptions.IntegrityError:
                        appId = self.appid.execute(conn, m["app_name"])
                tran.commit()
                #Now insert the config
                # Sort out the mess
                # We're having some problems with different threads
                # committing different pieces at the same time
                # This makes the output module config ID wrong
                # Trying to catch this via exception handling on duplication
                # Start a new transaction
                #global_tag is now required. YG 03/08/2011
                tran = conn.begin()
                try:
                    #get output module config id
                    cfgid = self.sm.increment(conn, "SEQ_OMC")
                    configObj = {'output_mod_config_id':cfgid , 'app_exec_id':appId,  'release_version_id':reId,  \
                                 'parameter_set_hash_id':pHId , 'output_module_label':m['output_module_label'],   \
                                 'global_tag':m['global_tag'], 'scenario':m.get('scenario', None),      \
                                 'creation_date':m.get('creation_date', None), 'create_by':m.get('create_by', None)}
                    self.otptModCfgin.execute(conn, configObj, tran)
                except exceptions.IntegrityError:
                    #There are another job inserted it just 1/100000 second earlier than you!!  YG 11/17/2010
                    cfgid = self.otptModCfgid.execute(conn, app = m["app_name"],
                                                  release_version = m["release_version"],
                                                  pset_hash = m["pset_hash"],
                                                  output_label = m["output_module_label"],
                                                  global_tag=m['global_tag'])
                    #tran.rollback()
                # End the transaction
                tran.commit()
                otptIdList.append(cfgid)
                key = m['app_name']+':'+m['release_version']+':'+m['pset_hash']+':'\
                      +m['output_module_label'] +':'+m['global_tag']
                self.datasetCache['conf'][key] = cfgid
        except Exception, ex:
            #self.logger.exception("%s DBSBlockInsert/output module config insertion. %s\n." %(DBSEXCEPTIONS['dbsException-2'], ex))
            tran.rollback()
            raise
        finally:
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
                self.insertDatasetWOannex(dataset = dataset, blockcontent = blockcontent,
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
                    try:
                        self.primdstpin.execute(conn, obj, tran)
                    except exceptions.IntegrityError:
                        primds["primary_ds_type_id"] = self.primdstpid.execute(conn, primds["primary_ds_type"],\
                                                transaction=tran)
                    except Exception, ex:
                        #self.logger.exception("%s DBSBlockInsert/Primary ds type insert. %s\n." %(DBSEXCEPTIONS['dbsException-2'], ex))
                        tran.rollback
                        raise
                #Now inserting primary ds. Clean up dao object befer inserting
                del primds["primary_ds_type"]
                primds["primary_ds_id"] = self.sm.increment(conn, "SEQ_PDS")
                try:
                    self.primdsin.execute(conn, primds, tran)
                except exceptions.IntegrityError:
                    primds["primary_ds_id"] = self.primdsid.execute(conn, primds["primary_ds_name"], \
                                                transaction=tran)
                except Exception, ex:   
                    #self.logger.exception("%s DBSBlockInsert/primary ds insert. %s\n." %(DBSEXCEPTIONS['dbsException-2'], ex))
                    tran.rollback
                    raise
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
                #self.logger.exception("%s DBSBlockInsert/processed ds insert. %s\n." %(DBSEXCEPTIONS['dbsException-2'], ex))
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
                try:
                    #insert acquisition era into db
                    aq['acquisition_era_id'] = self.sm.increment(conn,"SEQ_AQE")
                    aq['acquisition_era_name'] = aq['acquisition_era_name'].upper()
                    self.acqin.execute(conn, aq, tran)
                    dataset['acquisition_era_id'] = aq['acquisition_era_id']
                except exceptions.IntegrityError:
                    #ok, already in db
                    dataset['acquisition_era_id'] = self.acqid.execute(conn, aq['acquisition_era_name'].upper())
                except Exception, ex:
                    #self.logger.exception("%s DBSBlockInsert/acquisition era insert. %s\n." %(DBSEXCEPTIONS['dbsException-2'], ex))
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
                    pera['processing_version'] = pera['processing_version'].upper()
                    self.procsingin.execute(conn, pera, tran)
                    dataset['processing_era_id'] = pera['processing_era_id']
                except exceptions.IntegrityError:
                    #ok, already in db
                    dataset['processing_era_id'] = self.procsingid.execute(conn, pera['processing_version'])
                except Exception, ex:
                    #self.logger.exception("%s DBSBlockInsert/processing era insert. %s\n." %(DBSEXCEPTIONS['dbsException-2'], ex))
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
                phgId = self.phygrpid.execute(conn, phg, transaction=tran)
                if phgId <=0 :
                    #not in db yet, insert it
                    phygrp={'physics_group_id':self.sm.increment(conn,"SEQ_PG"), 'physics_group_name':phg}
                    try:
                        self.phygrpin.execute(conn, phygrp, tran)
                    except exceptions.IntegrityError:
                        phgId = self.phygrpid.execute(conn, phg, transaction=tran)
                    except Exception, ex:
                        #self.logger.exception("%s DBSBlockInsert/physics group insert. %s\n." %(DBSEXCEPTIONS['dbsException-2'], ex))
                        tran.rollback()
                        raise
                dataset['physics_group_id'] = phgId
                #self.logger.debug("***PHYSICS_GROUP_ID=%s***" %phgId)
            else:
                #no physics gruop for the dataset.
                pass
            del dataset['physics_group_name']
            #6 Deal with Data tier. A dataset must has a data tier
            dataT = dataset['data_tier_name'].upper()
            dataTId = self.tierid.execute(conn, dataT, transaction=tran)
            if dataTId <= 0 :
                #not in db. Insert the tier
                dataTId = self.sm.increment(conn,"SEQ_DT")
                theTier={'data_tier_id': dataTId,  'data_tier_name': dataT}
                try:                    
                    self.tierin.execute(conn, theTier, tran)
                except exceptions.IntegrityError:
                    dataTId = self.tierid.execute(conn, dataT, transaction=tran)
                except Exception, ex:
                    #self.logger.exception("%s DBSBlockInsert/data tier insert. %s\n." %(DBSEXCEPTIONS['dbsException-2'], ex))
                    tran.rollback()
                    raise
            dataset['data_tier_id'] = dataTId
            del dataset['data_tier_name']
            #7 Deal with dataset access type. A dataset must have a data type
            dsTp = dataset['dataset_access_type'].upper()
            dsTpId = self.datatypeid.execute(conn, dsTp, transaction=tran)
            if dsTpId <= 0 :
                #not in db. Insert the type
                dsTpId = self.sm.increment(conn,"SEQ_DTP")
                theType={'dataset_access_type':dsTp, 'dataset_access_type_id':dsTpId}
                try:
                    self.datatypein.execute(conn, theType, tran)
                except exceptions.IntegrityError:
                    dsTpId = self.datatypeid.execute(conn, dsTp, transaction=tran)
                except Exception, ex:
                    #self.logger.exception("%s DBSBlockInsert/data access type insert. %s\n." %(DBSEXCEPTIONS['dbsException-2'], ex))
                    tran.rollback()
                    raise
            dataset['dataset_access_type_id'] = dsTpId
            del dataset['dataset_access_type']
            tran.commit()
        except Exception, ex:
            #self.logger.exception("%s DBSBlockInsert/pre-dataset insert. %s\n." %(DBSEXCEPTIONS['dbsException-2'], ex))
            tran.rollback()
            raise
        try:
            #self.logger.debug("*** Trying to insert the dataset***")
            dataset['dataset_id'] = self.insertDatasetWOannex(dataset = dataset,
                                                           blockcontent = blockcontent,
                                                           otptIdList = otptIdList, conn = conn)
        except Exception:
            raise
        finally:
            conn.close()
            
        return dataset['dataset_id']



    def insertDatasetWOannex(self, dataset, blockcontent, otptIdList, conn, insertDataset = True):
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
                dataset['dataset_id'] = self.datasetid.execute(conn, dataset['dataset'])
                if dataset['dataset_id'] <= 0:
                    dataset['dataset_id'] = self.sm.increment(conn,"SEQ_DS")
                    try:
                        self.datasetin.execute(conn, dataset, tran)
                    except exceptions.IntegrityError:
                        dataset['dataset_id'] = self.datasetid.execute(conn, dataset['dataset'])
                    except Exception, ex:
                        #self.logger.exception("%s DBSBlockInsert/dataset insert. %s\n." %(DBSEXCEPTIONS['dbsException-2'], ex))
                        tran.rollback()
                        if conn:
                            conn.close()
                        raise
                
            #9 Fill Dataset Parentage
            if blockcontent.has_key('ds_parent_list'):
                dsPList = blockcontent['ds_parent_list']
                dsParentObjList=[]
                for p in dsPList:
                    dsParentObj={'this_dataset_id': dataset['dataset_id']\
                                 , 'parent_dataset_id' : self.datasetid.execute(conn, p['parent_dataset'])}
                    dsParentObjList.append(dsParentObj)
                #insert dataset parentage in bulk
                if dsParentObjList:
                    try:
                        self.dsparentin.execute(conn, dsParentObjList, tran)
                    except exceptions.IntegrityError:
                        #ok, already in db
                        #FIXME: What happends when there are partially in db? YG 11/17/2010
                        pass
                    except Exception, ex:
                        #self.logger.exception("%s DBSBlockInsert/dataset parent mapping insert. %s\n." %(DBSEXCEPTIONS['dbsException-2'], ex))
                        if tran:
                            tran.rollback()
                        raise
                                    
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
                    #FIXME: What happends when there are partially in db? YG 11/17/2010
                    pass
                except Exception, ex:
                    #self.logger.exception("%s DBSBlockInsert/ds & output module mapping insert. %s\n." %(DBSEXCEPTIONS['dbsException-2'], ex))
                    if tran:
                        tran.rollback()
                    raise 
            tran.commit()
        except exceptions.IntegrityError:
            # Then is it already in the database?
            # Maybe.  See what happens if we ignore
            pass
        except Exception, ex:
            #self.logger.exception("%s DBSBlockInsert/ds insert w/o Annex. %s\n." %(DBSEXCEPTIONS['dbsException-2'], ex))
            tran.rollback()
            raise

        return dataset['dataset_id']
