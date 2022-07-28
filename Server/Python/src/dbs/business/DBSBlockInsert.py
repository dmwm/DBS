#!/usr/bin/env python
"""
DBS  block insertion for WMAgent & Crab publishering
"""
from sqlalchemy import exc as exceptions
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
        #self.primdslist     = daofactory(classname="PrimaryDataset.List")
        self.primdsid       = daofactory(classname="PrimaryDataset.GetID")
        #self.datasetlist    = daofactory(classname="Dataset.List")
        self.datasetid      = daofactory(classname="Dataset.GetID")
        #self.blocklist      = daofactory(classname="Block.List")
        self.blockid        = daofactory(classname="Block.GetID")
        #self.filelist       = daofactory(classname="File.List")
        self.fileid         = daofactory(classname="File.GetID")
        self.filetypeid     = daofactory(classname="FileType.GetID")
        #self.fplist         = daofactory(classname="FileParent.List")
        #self.fllist         = daofactory(classname="FileLumi.List")
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
        self.dsparentin3     = daofactory(classname="DatasetParent.Insert3")
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
        except KeyError as ex:
            dbsExceptionHandler("dbsException-invalid-input2", "DBSBlockInsert/putBlock: \
                KeyError exception: %s. " %ex.args[0], self.logger.exception, 
	        "DBSBlockInsert/putBlock: KeyError exception: %s. " %ex.args[0]	)
        except Exception as ex:
            raise

    def insertBlockFile(self, blockcontent, datasetId, migration=False):
        tran = None
        conn = None
        try:
            block = blockcontent['block']
            newBlock = False
            #Insert the block
            conn = self.dbi.connection()
            tran = conn.begin()
            self.logger.info("Inserted block name: %s" %block['block_name'])
        except KeyError as ex:
            if tran:tran.rollback()
            if conn:conn.close()
            dbsExceptionHandler("dbsException-invalid-input2", "DBSBlockInsert/insetBlockFile: \
                KeyError exception: %s. " %ex.args[0], self.logger.exception, 
		"DBSBlockInsert/insetBlockFile:KeyError exception: %s. " %ex.args[0])
        except Exception as ex:
            if tran:tran.rollback()
            if conn:conn.close()
            raise
        try:
            block['block_id'] = self.sm.increment(conn, "SEQ_BK",)
            block['dataset_id'] =  datasetId
            if not migration:
                block['creation_date'] = dbsUtils().getTime()
                block['create_by'] = dbsUtils().getCreateBy()
                block['last_modification_date'] = dbsUtils().getTime()
                block['last_modified_by'] = dbsUtils().getCreateBy()
            self.blockin.execute(conn, block, tran)
            newBlock = True
        except exceptions.IntegrityError as ex:
            if (str(ex).find("ORA-00001") != -1 and str(ex).find("TUC_BK_BLOCK_NAME") != -1) or str(ex).lower().find("duplicate") != -1:
            #not sure what happens to WMAgent: Does it try to insert a
            #block again? YG 10/05/2010
            #Talked with Matt N: We should stop inserting this block now.
            #This means there is some trouble.
            #Throw exception to let the up layer know. YG 11/17/2010
                if tran:tran.rollback()
                if conn:conn.close()
                dbsExceptionHandler("dbsException-invalid-input2", "DBSBlockInsert/insertBlock. Block %s already exists." % (block['block_name']), self.logger.exception, "DBSBlockInsert/insertBlock. Block %s already exists." % (block['block_name']))
            elif str(ex).find("ORA-01400") > -1:
                if tran:tran.rollback()
                if conn:conn.close()
                dbsExceptionHandler('dbsException-missing-data',
                    'Missing data when insert Blocks. ',
                    self.logger.exception,
                    'Missing data when insert Blocks. '+ str(ex))
            else:
                if tran:tran.rollback()
                if conn:conn.close()
                dbsExceptionHandler('dbsException-invalid-input2',
                            'Invalid data when insert Blocks. ',
                            self.logger.exception,
                            'Invalid data when insert Blocks. '+ str(ex))

        #All Parentage will be deduced from file parentage.
        #Starting July 9 2018, We will allow to insert dataset parentage w/o file parentage because WMAgent needs to be updated 
        #in order to provide DBS with file parentage. So we will only put dataset parentage for now, and later there will be a script 
        # to fill the block and file parentags based on lumi sections.
        blockId = block['block_id']
        fileLumiList = []
        fileConfObjs = []
        logicalFileName = {}
        try:
            fileList = blockcontent['files']
            fileConfigList = blockcontent['file_conf_list']
            fileParentList = []
            dsParentListInpt = []
            if blockcontent.get('file_parent_list', []):
                fileParentList = blockcontent['file_parent_list']
            elif blockcontent.get('dataset_parent_list', []):
                dsParentListInpt = blockcontent['dataset_parent_list']
            if not fileList:
                return
        except KeyError as ex:
            if tran:tran.rollback()
            if conn:conn.close()
            dbsExceptionHandler("dbsException-invalid-input2", "DBSBlockInsert/insetFile: \
                KeyError exception: %s. " %ex.args[0],  self.logger.exception, "KeyError exception: %s." %ex.args[0] )
        except Exception as ex:
            if tran:tran.rollback()
            if conn:conn.close()
            raise 
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
                #We removed schema constraint that requires NOT NULL on check_sum. Now user can choose 
                #which one to use among check_sum, adler32 or md5. But at least one of them has to be there.
                # YG 5/11/2015. See github issues #468 for detail.
                if fileList[i]['check_sum'] is None and fileList[i]['adler32'] is None and fileList[i]['md5'] is None:
                    dbsExceptionHandler('dbsException-invalid-input2',
                       'Invalid checksum when insert File. One of these checksums needed: check_sum, adler32 or md5',
                       self.logger.exception, 'Invalid checksum when insert File. One of these checksums needed: check_sum,adler32 or md5')
                #fileList[i]['creation_date'] = fileList[i].get('creation_date', None) #see ticket 965
                #fileList[i]['create_by'] = fileList[i].get('create_by', None)
                if not migration:
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
        except KeyError as ex:
            if tran:tran.rollback()
            if conn:conn.close() 
            dbsExceptionHandler("dbsException-invalid-input2", "DBSBlockInsert/FileProperty: \
                KeyError exception: %s. " %ex.args[0] )
        except Exception as ex:
            if tran:tran.rollback()
            if conn:conn.close()
            raise
        try:
            #deal with file parentage.
            #At the meantime, we need to build block and dataset parentage.
            #All parentage is deduced from file paretage,
            #10/16/2011
            #
            #Starting July 9 2018, we will allow dataset parentage only insert into DBS w/o file or block parentages. 
            nfileparent = len(fileParentList)
            bkParentList = []
            dsParentList = []
            hasFparentage = False
            hasDSparentage = False
            if fileParentList:
                for k in range(nfileparent):
                    hasFparentage = True
                    if migration:
                        fileParentList[k]['this_file_id'] = logicalFileName[fileParentList[k]['this_logical_file_name']]
                        fileParentList[k].pop('this_logical_file_name', None)
                        fileParentList[k].pop('parent_file_id', None)
                    else:
                        fileParentList[k]['this_file_id'] = logicalFileName[fileParentList[k]['logical_file_name']]
                        del fileParentList[k]['logical_file_name']
                    bkParentage2insert={'this_block_id' : blockId, 'parent_logical_file_name': fileParentList[k]['parent_logical_file_name']}
                    dsParent2Insert = {'this_dataset_id' : datasetId, 'parent_logical_file_name': fileParentList[k]['parent_logical_file_name']}
                    if not any(d.get('parent_logical_file_name') == bkParentage2insert['parent_logical_file_name'] for d in bkParentList):
                        #not exist yet
                        bkParentList.append(bkParentage2insert)
                        dsParentList.append(dsParent2Insert)
            elif dsParentListInpt:
                # we are not going to migrate dataset parentage only data from production DB to others. 
                # This need to have the block to be open instead of closed. But WMAgent developer disgreed on keeping the block open.
                # YG. July 10, 2018 
                hasDSparentage = True
                for k in range(len(dsParentListInpt)): 
                    dsParent2Insert = {'this_dataset_id' : datasetId, 'parent_dataset': dsParentListInpt[k]}
                    if not any(d.get('parent_dataset') == dsParent2Insert['parent_dataset'] for d in dsParentList):
                        dsParentList.append(dsParent2Insert)
            else:
                pass 

            #deal with file config
            for fc in fileConfigList:
		
                key = (fc['app_name'] + ':' + fc['release_version'] + ':' +
                       fc['pset_hash'] + ':' +
                       fc['output_module_label'] + ':' + fc['global_tag'])
                if not key in (self.datasetCache['conf']).keys():
                    #we expect the config is inserted when the dataset is in.
                    if tran:tran.rollback()
                    if conn:conn.close()
                    dbsExceptionHandler('dbsException-missing-data', ' Required Configuration application name, release version,\
                        pset hash and global tag: %s, %s\
                        ,%s,%s not found in DB' %(fc['app_name'], fc['release_version'], fc['pset_hash'], fc['global_tag']))
                fcObj = {'file_id' : logicalFileName[fc['lfn']],
                         'output_mod_config_id': self.datasetCache['conf'][key]}
                fileConfObjs.append(fcObj)
        except KeyError as ex:
            if tran:tran.rollback()
            if conn:conn.close()
            dbsExceptionHandler("dbsException-invalid-input2", "DBSBlockInsert/FileParents: \
                KeyError exception: %s. " %ex.args[0], self.logger.exception, "DBSBlockInsert/FileParents:\
		KeyError exception: %s. " %ex.args[0])
        except Exception as ex:
            if tran:tran.rollback()
            if conn: conn.close()
            raise
        try:
            #now we build everything to insert the files.
            #tran = conn.begin()
            try:
                #insert files
                if fileList:
                    self.filein.execute(conn, fileList, tran)
            except exceptions.IntegrityError as ex:
                if tran:tran.rollback()
                if conn:conn.close()
                if (str(ex).find("ORA-00001") != -1 and str(ex).find("TUC_FL_LOGICAL_FILE_NAME"))\
                    or str(ex).lower().find("duplicate") != -1:
                    dbsExceptionHandler('dbsException-invalid-input2', 'Duplicated lfn name when inserting\
                    files. ', self.logger.exception, 'Duplicated lfn name when inserting files. '+str(ex))
                else:
                    dbsExceptionHandler('dbsException-invalid-input2',
                        'Invalid data when insert files.  ',
                        self.logger.exception,
                        'Invalid data when insert file. '+ str(ex))
            try:
                #insert file parents
                if hasFparentage and fileParentList:
                    self.fparentin.execute(conn, fileParentList, tran)
            except exceptions.IntegrityError as ex:
                if tran:tran.rollback()
                if conn:conn.close()
                if str(ex).find("ORA-01400") > -1:
                    dbsExceptionHandler('dbsException-missing-data',
                        'Missing data when insert file parent. ', self.logger.exception,
                        'Missing data when insert file parent. '+ str(ex))
                else:
                    dbsExceptionHandler('dbsException-invalid-input2',
                        'Invalid data when insert file parent.  ', self.logger.exception,
                        'Invalid data when insert file parent. '+ str(ex))

            try:
                #insert file lumi
                if fileLumiList:
                    self.flumiin.execute(conn, fileLumiList, tran)
            except exceptions.IntegrityError as ex:
                if tran:tran.rollback()
                if conn:conn.close()
                if str(ex).find("ORA-01400") > -1:
                    dbsExceptionHandler('dbsException-missing-data',
                        'Missing data when insert file lumi. ', self.logger.exception,
                        'Missing data when insert file lumi. '+ str(ex))
                else:
                    dbsExceptionHandler('dbsException-invalid-input2',
                        'Invalid data when insert file lumi.  ', self.logger.exception,
                        'Invalid data when insert file lumi. '+ str(ex))

            try:
                #insert file configration
                if fileConfObjs:
                    self.fconfigin.execute(conn, fileConfObjs, tran)
            except exceptions.IntegrityError as ex:
                if tran:tran.rollback()
                if conn:conn.close()
                if str(ex).find("ORA-01400") > -1:
                    dbsExceptionHandler('dbsException-missing-data',
                        'Missing data when insert file configuration. ', self.logger.exception,
                        'Missing data when insert file configuration. '+ str(ex))
                else:
                    dbsExceptionHandler('dbsException-invalid-input2',
                        'Invalid data when insert file config.  ', self.logger.exception,
                        'Invalid data when insert:file config. '+ str(ex))

            #insert bk and dataset parentage
            #we cannot do bulk insertion for the block and dataset parentage because they may be duplicated.
            lbk = len(bkParentList)
            dsk = len(dsParentList)
            for k in range(lbk):
                try:
                    # We only have block parentage when file prentage presents. 
                    if hasFparentage: self.blkparentin2.execute(conn, bkParentList[k], transaction=tran)
                except exceptions.IntegrityError as ex:
                    if (str(ex).find("ORA-00001") != -1 and str(ex).find("PK_BP"))\
                         or str(ex).lower().find("duplicate") != -1:
                        pass
                    elif str(ex).find("ORA-01400") > -1:
                        if tran:tran.rollback()
                        if conn:conn.close()
                        dbsExceptionHandler('dbsException-missing-data',
                            'Missing data when insert Block_Parents. ',
                            self.logger.exception,
                            'Missing data when insert Block_Parents. '+ str(ex))
                    else:
                        if tran:tran.rollback()
                        if conn:conn.close()
                        dbsExceptionHandler('dbsException-invalid-input2',
                            'Invalid data when insert Block_Parents. ',
                            self.logger.exception,
                            'Invalid data when insert Block_Parents. '+ str(ex))

            for k in range(dsk):
                try:
                    if hasFparentage: 
                        self.dsparentin2.execute(conn, dsParentList[k], transaction=tran)
                    elif hasDSparentage:
                        self.dsparentin3.execute(conn, dsParentList[k], transaction=tran)
                    else:
                        pass
                except exceptions.IntegrityError as ex:
                    if (str(ex).find("ORA-00001") != -1 and str(ex).find("PK_DP"))\
                        or str(ex).lower().find("duplicate") != -1:
                        pass
                    elif str(ex).find("ORA-01400") > -1:
                        if tran:tran.rollback()
                        if conn:conn.close()
                        dbsExceptionHandler("dbsException-missing-data", " Missing data when inserting to dataset_parents. ", self.logger.exception, str(ex))
                    else:
                        if tran:tran.rollback()
                        if conn:conn.close()
                        dbsExceptionHandler('dbsException-invalid-input2',
                            'Invalid data when insert Dataset_Parents. ',
                            self.logger.exception,
                            'Invalid data when insert Dataset_Parents. '+ str(ex))
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
        except KeyError as ex:
            if conn:conn.close()
            dbsExceptionHandler("dbsException-invalid-input2", "DBSBlockInsert/insertOutputModuleConfig: \
                KeyError exception: %s. " %ex.args[0], self.logger.exception,
	        "DBSBlockInsert/insertOutputModuleConfig: KeyError exception: %s. " %ex.args[0]	)
        except Exception as ex:
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
                except KeyError as ex:
                    if tran:tran.rollback()
                    if conn:conn.close()
                    dbsExceptionHandler("dbsException-invalid-input2", "DBSBlockInsert/insertOutputModuleConfig: \
                                         KeyError exception: %s. " %ex.args[0],
					 self.logger.exception, 
					"DBSBlockInsert/insertOutputModuleConfig: KeyError exception: %s. " %ex.args[0])
                except exceptions.IntegrityError as ex:
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
                            except exceptions.IntegrityError as ex:
                                if (str(ex).find("ORA-00001") != -1 and str(ex).find("TUC_OMC_1"))\
                                        or str(ex).lower().find("duplicate") != -1:
                                    pass
                                else:
                                    if tran:tran.rollback()
                                    if conn:conn.close()
                                    dbsExceptionHandler('dbsException-invalid-input2',
                                        'Invalid data when insert Configure. ',
                                        self.logger.exception,
                                        'Invalid data when insert Configure. '+ str(ex))
                    elif str(ex).find("ORA-01400") > -1:
                        if tran:tran.rollback()
                        if conn:conn.close()
                        dbsExceptionHandler("dbsException-missing-data", "Missing data when inserting Configure. ", 
				self.logger.exception, str(ex))
                    else:
                        if tran:tran.rollback()
                        if conn:conn.close()
                        dbsExceptionHandler('dbsException-invalid-input2',
                            'Invalid data when insert Configure. ',
                            self.logger.exception,
                            'Invalid data when insert Configure. '+ str(ex))
                except exceptions as ex3:
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
        except KeyError as ex:
            if conn:conn.close()
            dbsExceptionHandler("dbsException-invalid-input2", "DBSBlockInsert/InsertDataset: Dataset is required.\
                Exception: %s.  troubled dataset are: %s" %(ex.args[0], dataset),
	        self.logger.exception, "DBSBlockInsert/InsertDataset: Dataset is required.\
                Exception: %s.  troubled dataset are: %s" %(ex.args[0], dataset	))
        except Exception as ex1:
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
        primary_ds_name = ''
        try:
            #1. Deal with primary dataset. Most primary datasets are
            #pre-installed in db
            primds = blockcontent["primds"]
            primary_ds_name = primds["primary_ds_name"]
            primds["primary_ds_id"] = self.primdsid.execute(conn,
                                      primds["primary_ds_name"], transaction=tran)
            if primds["primary_ds_id"] <= 0:
                #primary dataset is not in db yet.
                try:
                    primds["primary_ds_id"] = self.sm.increment(conn, "SEQ_PDS")
                    primds["creation_date"] = primds.get("creation_date", dbsUtils().getTime())
                    if not migration:
                        primds["create_by"] = dbsUtils().getCreateBy()
                    self.primdsin.execute(conn, primds, tran)
                except exceptions.IntegrityError as ex:
                    if (str(ex).find("ORA-00001") != -1 and str(ex).find("TUC_PDS_PRIMARY_DS_NAME") != -1)\
                        or str(ex).lower().find("duplicate") !=-1:
                        primds["primary_ds_id"] = self.primdsid.execute(conn,
                                                primds["primary_ds_name"],
                                                transaction=tran)
                        if primds["primary_ds_id"] <= 0:
                            if tran:tran.rollback()
                            if conn:conn.close()
                            dbsExceptionHandler('dbsException-conflict-data',
                                'Primary dataset not yet inserted by concurrent insert. ',
                                self.logger.exception,
                                'Primary dataset not yet inserted by concurrent insert. '+ str(ex))

                    elif str(ex).find("ORA-01400") > -1:
                        if tran:tran.rollback()
                        if conn:conn.close()
                        dbsExceptionHandler('dbsException-missing-data',
                            'Missing data when insert primary_datasets. ',
                            self.logger.exception,
                            'Missing data when insert primary_datasets. '+ str(ex))
                    else:
                        if tran:tran.rollback()
                        if conn:conn.close()
                        dbsExceptionHandler('dbsException-invalid-input2',
                            'Invalid data when insert primary_datasets. ',
                            self.logger.exception,
                            'Invalid data when insert primary_datasets. '+ str(ex))
                except Exception as ex:
                    if tran:tran.rollback()
                    if conn:conn.close()
                    raise
            dataset['primary_ds_id'] = primds["primary_ds_id"]
            #2 Deal with processed ds
            #processed ds is handled inside of dataset insertion, However we need to make sure it is formatted correctly.
            #processed_ds_name is not required pre-exist in the db. will insert with the dataset if not in yet
            #
            #         processed_ds_name=acquisition_era_name[-filter_name][-processing_str]-vprocessing_version
            #         Note [-filterName] is new as 4/30/2012. See ticket #3655. YG
            #
            #althrough acquisition era and processing version is not required for a dataset
            #in the schema(the schema is build this way because
            #we need to accommodate the DBS2 data), but we impose the requirement on the API.
            #So both acquisition and processing eras are required.
            #We do the format checking after we deal with acquisition era and processing era.
            #
            #YG 12/07/2011  TK-362

            #3 Deal with Acquisition era
            aq = blockcontent.get('acquisition_era', {})
            has_acquisition_era_name = 'acquisition_era_name' in aq
            has_start_date = 'start_date' in aq

            def insert_acquisition_era():
                try:
                    #insert acquisition era into db
                    aq['acquisition_era_id'] = self.sm.increment(conn, "SEQ_AQE")
                    self.acqin.execute(conn, aq, tran)
                    dataset['acquisition_era_id'] = aq['acquisition_era_id']
                except exceptions.IntegrityError as ei:
                    #ORA-01400: cannot insert NULL into required columns, usually it is the NULL on start_date
                    if "ORA-01400" in str(ei) :
                        if tran:tran.rollback()
                        if conn:conn.close()
                        dbsExceptionHandler("dbsException-invalid-input2",
                                            "BlockInsert: acquisition_era_name and start_date are required. \
                                            NULL was received from user input. Please correct your data.")
                    #ok, already in db?
                    if (str(ei).find("ORA-00001") != -1 and str(ei).find("TUC_AQE_ACQUISITION_ERA_NAME") != -1)\
                        or str(ei).lower().find("duplicate") !=-1:
                        dataset['acquisition_era_id'] = self.acqid.execute(conn, aq['acquisition_era_name'])
                        if dataset['acquisition_era_id'] <= 0:
                            if tran:tran.rollback()
                            if conn:conn.close()
                            dbsExceptionHandler("dbsException-invalid-input2", "BlockInsert: \
Check the spelling of acquisition Era name. The db may already have the same \
acquisition era, but with different cases.", self.logger.exception, "BlockInsert: \
Check the spelling of acquisition Era name. The db may already have the same \
acquisition era, but with different cases.")
                    else:
                        if tran:tran.rollback()
                        if conn:conn.close()
                        dbsExceptionHandler('dbsException-invalid-input2',
                            'Invalid data when insert acquisition_eras . ',
                            self.logger.exception,
                            'Invalid data when insert acquisition_eras. '+ str(ei))
 
                except Exception:
                    if tran:tran.rollback()
                    if conn:conn.close()
                    raise

            if has_acquisition_era_name and has_start_date:
                insert_acquisition_era()

            elif migration and not has_acquisition_era_name:
                #if no processing era is available, for example for old DBS 2 data, skip insertion
                aq['acquisition_era_id'] = None
                dataset['acquisition_era_id'] = None

            elif migration and not aq['start_date']:
                aq['start_date'] = 0
                insert_acquisition_era()

            else:
                if tran:tran.rollback()
                if conn:conn.close()
                dbsExceptionHandler("dbsException-invalid-input2", "BlockInsert: Acquisition Era is required", 
			self.logger.exception, "BlockInsert: Acquisition Era is required")

            #4 Deal with Processing era
            pera = blockcontent.get('processing_era', {})

            if 'processing_version' in pera:
                try:
                    #insert processing era into db
                    pera['processing_era_id'] = self.sm.increment(conn, "SEQ_PE")
                    #pera['processing_version'] = pera['processing_version'].upper()
                    self.procsingin.execute(conn, pera, tran)
                    dataset['processing_era_id'] = pera['processing_era_id']
                except exceptions.IntegrityError as ex:
                    if (str(ex).find("ORA-00001: unique constraint") != -1 and \
                                str(ex).find("TUC_PE_PROCESSING_VERSION") != -1) or \
                                str(ex).lower().find("duplicate") !=-1:
                        #ok, already in db
                        dataset['processing_era_id'] = self.procsingid.execute(conn, pera['processing_version'])
                    elif str(ex).find("ORA-01400") > -1:
                        if tran:tran.rollback()
                        if conn:conn.close()
                        dbsExceptionHandler('dbsException-missing-data',
                            'Missing data when insert processing_eras. ',
                            self.logger.exception,
                            'Missing data when insert Processing_eras. '+ str(ex))
                    else:
                        if tran: tran.rollback()
                        if conn: conn.close()
                        dbsExceptionHandler('dbsException-invalid-input2',
                            'Invalid data when insert Processing_ears. ',
                            self.logger.exception,
                            'Invalid data when insert Processing_eras. '+ str(ex))
                except Exception as ex:
                    if tran: tran.rollback()
                    if conn: conn.close()
                    raise
            elif migration:
                #if no processing era is available, for example for old DBS 2 data, skip insertion
                pera['processing_era_id'] = None
                dataset['processing_era_id'] = None
            else:
                if tran: tran.rollback()
                if conn: conn.close()
                dbsExceptionHandler('dbsException-invalid-input2', 'BlockInsert:processing version is required')

            #Make sure processed_ds_name is right format.
            #processed_ds_name=acquisition_era_name[-filter_name][-processing_str]-vprocessing_version
            #In order to accommodate DBS2 data for migration, we turn off this check in migration.
            #These will not cause any problem to none DBS2 data because when we migration, the none DBS2 data is
            #already checked when they were inserted into the source dbs.  YG 7/12/2012
            if not migration and aq["acquisition_era_name"] != "CRAB" and aq["acquisition_era_name"] != "LHE":
                erals=dataset["processed_ds_name"].rsplit('-')
                if erals[0] != aq["acquisition_era_name"] or erals[len(erals)-1] != "%s%s"%("v", pera["processing_version"]):
                    if tran:tran.rollback()
                    if conn:conn.close()
                    dbsExceptionHandler('dbsException-invalid-input2', "BlockInsert:\
                   processed_ds_name=acquisition_era_name[-filter_name][-processing_str]-vprocessing_version must be satisified.",
		   self.logger.exception, 
		   "BlockInsert: processed_ds_name=acquisition_era_name[-filter_name][-processing_str]-vprocessing_version must be satisified."	)

            #So far so good, let's commit first 4 db activities before going on.
            tran.commit()
        except KeyError as ex:
            if tran:tran.rollback()
            if conn:conn.close()
            dbsExceptionHandler("dbsException-invalid-input2", "DBSBlockInsert/insertOutputModuleConfig: \
                                         KeyError exception: %s. " %ex.args[0], self.logger.exception,
				         "DBSBlockInsert/insertOutputModuleConfig: KeyError exception: %s." %ex.args[0]	)            
        except:
        
            if tran:tran.rollback()
            if conn:conn.close()
            raise

        #Continue for the rest.
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
                    except exceptions.IntegrityError as ex:
                        if str(ex).find("ORA-00001") != -1 and str(ex).find("PK_PG") != -1:
                            if tran:tran.rollback()
                            if conn:conn.close()
                            dbsExceptionHandler(message='InsertPhysicsGroup Error', logger=self.logger.exception, serverError="InsertPhysicsGroup: "+ str(ex))
                        if (str(ex).find("ORA-00001") != -1 and str(ex).find("TUC_PG_PHYSICS_GROUP_NAME") != -1) or\
                            str(ex).lower().find("duplicate") != -1:
                            phgId = self.phygrpid.execute(conn, phg, transaction=tran)
                            if phgId <= 0:
                                if tran:tran.rollback()
                                if conn:conn.close()
                                dbsExceptionHandler(message='InsertPhysicsGroup Error ', logger=self.logger.exception, serverError="InsertPhysicsGroup: "+str(ex))
                        elif str(ex).find("ORA-01400") > -1:
                            if tran:tran.rollback()
                            if conn:conn.close()
                            dbsExceptionHandler('dbsException-missing-data',
                                'Missing data when insert Physics_groups. ',
                                self.logger.exception,
                                'Missing data when insert Physics_groups. '+ str(ex))
                        else:
                            if tran: tran.rollback()
                            if conn: conn.close()
                            dbsExceptionHandler('dbsException-invalid-input2',
                            'Invalid data when insert Physics_groups. ',
                            self.logger.exception,
                            'Invalid data when insert Physics_groups. '+ str(ex))                        
                    except Exception as ex:
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
            #We no longer handle Tier inside dataset insert. If a data tier is no in DBS before the dataset 
            # is inserted. We will report error back to the user as missing data. See github issue #466.
            # This is to prevent users to insert random data tiers into phys* DB. YG May-15-2015
            dtId = 0
            dtId = self.tierid.execute(conn, dataset['data_tier_name'])   
            #When no data tier found, it return tier id -1
            if dtId <= 0:
                dbsExceptionHandler('dbsException-missing-data', 'Required data tier %s not found in DBS when insert dataset. Ask your admin adding the tier before insert/migrate the block/dataset.' %dataset['data_tier_name'],
                         self.logger.exception, 'Required data tier not found in DBS when insert dataset. ')
            #7 Deal with dataset access type. A dataset must have a data type
            dataset['dataset_access_type'] = dataset['dataset_access_type'].upper()
            #handle dataset access type inside dataset insertion with Inser2.
            tran.commit()
        except Exception as ex:
            if tran:tran.rollback()
            if conn:conn.close()
            raise
        #Before we insert the dataset, we need to make sure dataset=/primary_dataset_name/processed_dataset_name/data_tier 
        d2 = dataset['dataset'].rsplit('/')
        if (d2[1] != primary_ds_name or d2[2] != dataset["processed_ds_name"] or d2[3] != dataset['data_tier_name']):
            if tran:tran.rollback()
            if conn:conn.close()
            dbsExceptionHandler('dbsException-invalid-input2', 
                                'dataset=/primary_dataset_name/processed_dataset_name/data_tier is not matched.',
                                self.logger.exception, 'dataset=/primary_dataset_name/processed_dataset_name/data_tier is not matched.')      
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
                    dataset['dataset_id'] = self.sm.increment(conn, "SEQ_DS")
                    if not migration:
                        dataset['last_modified_by'] = dbsUtils().getCreateBy()
                        dataset['create_by'] = dbsUtils().getCreateBy()
                        dataset['creation_date'] = dataset.get('creation_date', dbsUtils().getTime())
                        dataset['last_modification_date'] = dataset.get('last_modification_date', dbsUtils().getTime())
                    dataset['xtcrosssection'] = dataset.get('xtcrosssection', None)
                    dataset['prep_id'] = dataset.get('prep_id', None)
                    try:
                        self.datasetin.execute(conn, dataset, tran)
                    except exceptions.IntegrityError as ei:
                        if str(ei).find("ORA-00001") != -1 or str(ei).lower().find("duplicate") !=-1:
                            if conn.closed:
                                conn = self.dbi.connection()
                            dataset['dataset_id'] = self.datasetid.execute(conn, dataset['dataset'])
                            if dataset['dataset_id'] <= 0:
                                if tran:tran.rollback()
                                if conn:conn.close()
                                dbsExceptionHandler('dbsException-conflict-data',
                                                    'Dataset/[processed DS]/[dataset access type] not yet inserted by concurrent insert. ',
                                                    self.logger.exception,
                                                    'Dataset/[processed DS]/[dataset access type] not yet inserted by concurrent insert. '+ str(ei))
                        elif str(ei).find("ORA-01400") > -1:
                            if tran:tran.rollback()
                            if conn:conn.close()
                            dbsExceptionHandler('dbsException-missing-data',
                                'Missing data when insert Datasets. ',
                                self.logger.exception,
                                'Missing data when insert Datasets. '+ str(ei))
                        else:
                            if tran: tran.rollback()
                            if conn: conn.close()
                            dbsExceptionHandler('dbsException-invalid-input2',
                            'Invalid data when insert Datasets. ',
                            self.logger.exception,
                            'Invalid data when insert Datasets. '+ str(ei))

                    except Exception:
                        #should catch all above exception to rollback. YG Jan 17, 2013
                        if tran:tran.rollback()
                        if conn:conn.close()
                        raise

            #9 Fill Dataset Parentage
            #All parentage are deduced from file parentage.

            #10 Before we commit, make dataset and output module configuration
            #mapping.  We have to try to fill the map even if dataset is
            #already in dest db
            for c in otptIdList:
                try:
                    dcObj = {
                             'dataset_id' : dataset['dataset_id'],
                             'output_mod_config_id' : c }
                    self.dcin.execute(conn, dcObj, tran)
                except exceptions.IntegrityError as ei:
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
                        dbsExceptionHandler('dbsException-invalid-input2',
                            'Invalid data when insert dataset_configs. ',
                            self.logger.exception,
                            'Invalid data when insert dataset_configs. '+ str(ei))
                except Exception as ex:
                    if tran:tran.rollback()
                    if conn:conn.close()
                    raise
            #Now commit everything.
            tran.commit()
        except exceptions.IntegrityError as ei:
            # Then is it already in the database?
            # Not really. We have to check it again. YG Jan 17, 2013
            # we don't check the unique key here, since there are more than one unique key might
            # be violated: such as data_tier, processed_dataset, dataset_access_types.
            if str(ei).find("ORA-00001") != -1 or str(ei).lower().find("duplicate")!=-1:
                # For now, we assume most cases are the same dataset was instered by different thread. If not,
                # one has to call the insert dataset again. But we think this is a rare case and let the second
                # DBSBlockInsert call fix it if it happens.
                if conn.closed:
                    conn = self.dbi.connection()
                dataset_id = self.datasetid.execute(conn, dataset['dataset'])
                if dataset_id <= 0:
                    dbsExceptionHandler('dbsException-conflict-data',
                                        'Dataset not yet inserted by concurrent insert',
                                        self.logger.exception,
                                        'Dataset not yet inserted by concurrent insert')

                else:
                    dataset['dataset_id'] = dataset_id
            else:
                if tran:tran.rollback()
                if conn:conn.close()
                dbsExceptionHandler('dbsException-invalid-input2',
                    'Invalid data when insert Datasets. ',
                    self.logger.exception,
                    'Invalid data when insert Datasets. '+ str(ei))
        except Exception as ex:
            if tran:tran.rollback()
            if conn:conn.close()
            raise
        finally:
            if tran:tran.rollback()
            if conn:conn.close()
        return dataset['dataset_id']
