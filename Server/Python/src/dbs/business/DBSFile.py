#!/usr/bin/env python
#pylint: disable=C0103
"""
This module provides business object class to interact with File.
"""

from WMCore.DAOFactory import DAOFactory
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from sqlalchemy.exc import IntegrityError as SQLAlchemyIntegrityError
from dbs.utils.dbsUtils import dbsUtils

class DBSFile:
    """
    File business object class
    """
    def __init__(self, logger, dbi, owner):
        daofactory = DAOFactory(package='dbs.dao', logger=logger,
                                dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi

        self.filelist = daofactory(classname="File.List")
        self.filebrieflist = daofactory(classname="File.BriefList")
        self.filesummarylist = daofactory(classname="File.SummaryList")
        self.sm = daofactory(classname = "SequenceManager")
        self.filein = daofactory(classname = "File.Insert")
        self.flumiin = daofactory(classname = "FileLumi.Insert")
        self.fparentin = daofactory(classname = "FileParent.Insert")
        self.fileid = daofactory(classname = "File.GetID")
        self.datasetid = daofactory(classname = "Dataset.GetID")
        self.blockid = daofactory(classname = "Block.GetID")
        self.blocklist = daofactory(classname = "Block.List")
        self.ftypeid = daofactory(classname = "FileType.GetID")
        self.fpbdlist = daofactory(classname = "FileParentBlock.List")
        self.blkparentin = daofactory(classname = "BlockParent.Insert2")
        self.dsparentin = daofactory(classname = "DatasetParent.Insert2")
        self.blkstats = daofactory(classname = "Block.ListStats")
        self.blkstatsin = daofactory(classname = "Block.UpdateStats")
        self.outconfigid = daofactory(classname='OutputModuleConfig.GetID')
        self.fconfigin = daofactory(classname='FileOutputMod_config.Insert')
        self.updatestatus = daofactory(classname='File.UpdateStatus')
        self.dsconfigids = daofactory(
                            classname='DatasetOutputMod_config.GetDSConfigs')
        self.fileparentlist = daofactory(classname="FileParent.List")
        self.filechildlist = daofactory(classname="FileParent.ListChild")
        self.filelumilist = daofactory(classname="FileLumi.List")
        self.filebufin = daofactory(classname = "FileBuffer.Insert")

    def listFileLumis(self, logical_file_name="", block_name="", run_num=-1, validFileOnly=0, input_body=-1):
        """
        optional parameter: logical_file_name, block_name, validFileOnly
        returns: logical_file_name, file_lumi_id, run_num, lumi_section_num
        """
        if((logical_file_name=='' or '*'in logical_file_name or '%' in logical_file_name) \
            and (block_name=='' or '*' in block_name or '%' in block_name) and input_body==-1 ):
            dbsExceptionHandler('dbsException-invalid-input', \
                "Fully specified logical_file_name or block_name is required if GET is called. No wildcards are allowed.",
		 self.logger.exception, "Fully specified logical_file_name or block_name is required if GET is called. No wildcards are allowed.")
        elif input_body != -1 :
            try:
                logical_file_name = input_body["logical_file_name"]
                run_num = input_body.get("run_num", -1)
                validFileOnly = input_body.get("validFileOnly", 0)
                block_name = ""
            except cjson.DecodeError as de:
                msg = "business/listFileLumis requires at least a list of logical_file_name. %s" % de
                dbsExceptionHandler('dbsException-invalid-input2', "Invalid input", self.logger.exception, msg)
        elif input_body != -1 and (logical_file_name is not None or block_name is not None): 
            dbsExceptionHandler('dbsException-invalid-input', "listFileLumis may have input in the command or in the payload, not mixed.", self.logger.exception, "listFileLumis may have input in the command or in the payload, not mixed.")

        with self.dbi.connection() as conn:
            for item in  self.filelumilist.execute(conn, logical_file_name, block_name, run_num, validFileOnly=validFileOnly):
                yield item

    def listFileSummary(self, block_name="", dataset="", run_num=-1, validFileOnly=0):
        """
        required parameter: full block_name or dataset name. No wildcards allowed. run_num is optional.
        """
        if not block_name and not dataset:
            msg =  "Block_name or dataset is required for listFileSummary API"
            dbsExceptionHandler('dbsException-invalid-input', msg, self.logger.exception)
        if '%' in block_name or '*' in block_name or '%' in dataset or '*' in dataset:
            msg = "No wildcard is allowed in block_name or dataset for filesummaries API"
            dbsExceptionHandler('dbsException-invalid-input', msg, self.logger.exception)
        #
        with self.dbi.connection() as conn:
            for item in self.filesummarylist.execute(conn, block_name, dataset, run_num,
                validFileOnly=validFileOnly):
                if item['num_file']==0 and item['num_block']==0 \
                        and item['num_event']==0 and item['file_size']==0:
                    pass
                else:
                    yield item
    def listFileParents(self, logical_file_name="", block_id=0, block_name=""):
        """
        required parameter: logical_file_name or block_name
        returns: this_logical_file_name, parent_logical_file_name, parent_file_id
        """
        #self.logger.debug("lfn %s, block_name %s, block_id :%s" % (logical_file_name, block_name, block_id))
        if not logical_file_name and not block_name and not block_id:
            dbsExceptionHandler('dbsException-invalid-input', \
                "Logical_file_name, block_id or block_name is required for fileparents api", self.logger.exception )
        with self.dbi.connection() as conn:
            sqlresult = self.fileparentlist.execute(conn, logical_file_name, block_id, block_name)
            d = {}
            #self.logger.debug(sqlresult)
            for i in sqlresult:
                k = i['this_logical_file_name']
                v = i['parent_logical_file_name']
                d.setdefault(k, []).append(v)
            for k, v in d.items():
                yield {'logical_file_name':k, 'parent_logical_file_name': v}
            del d    

    def listFileChildren(self, logical_file_name='', block_name='', block_id=0):
        """
        required parameter: logical_file_name or block_name or block_id
        returns: logical_file_name, child_logical_file_name, parent_file_id
        """
        conn = self.dbi.connection()
        try:
            if not logical_file_name and not block_name and not block_id:
                dbsExceptionHandler('dbsException-invalid-input',\
                        "Logical_file_name, block_id or block_name is required for listFileChildren api")
            sqlresult = self.filechildlist.execute(conn, logical_file_name, block_name, block_id)
            d = {}
            result = []
            for i in range(len(sqlresult)):
                k = sqlresult[i]['logical_file_name']
                v = sqlresult[i]['child_logical_file_name']
                if k in d:
                    d[k].append(v)
                else:
                    d[k] = [v]
            for k, v in d.items():
                r = {'logical_file_name':k, 'child_logical_file_name': v}
                result.append(r)
            return result
        finally:
            if conn:
                conn.close()

    def updateStatus(self, logical_file_name, is_file_valid, lost, dataset):
        """
        Used to toggle the status of a file from is_file_valid=1 (valid) to is_file_valid=0 (invalid)
        """

        conn = self.dbi.connection()
        trans = conn.begin()
        try :
            self.updatestatus.execute(conn, logical_file_name, is_file_valid, lost, dataset, trans)
            trans.commit()
            trans = None
        except Exception as ex:
            if trans:
                trans.rollback()
                trans = None
            raise ex

        finally:
            if trans:
                trans.rollback()
            if conn:
                conn.close()

    def listFiles(self, dataset="", block_name="", logical_file_name="",
                  release_version="", pset_hash="", app_name="",
                  output_module_label="",  run_num=-1,
                  origin_site_name="", lumi_list=[], detail=False, validFileOnly=0, input_body=-1):
        """
        One of below parameter groups must be present:
        non-patterned dataset, non-patterned block, non-patterned dataset with lfn,  non-patterned block with lfn,
        non-patterned lfn
	non-patterned lfn list
        """
        if input_body != -1 :
	    try:
                logical_file_name = input_body.get("logical_file_name", "")
                run_num = input_body.get("run_num", -1)
                validFileOnly = input_body.get("validFileOnly", 0)
		detail = input_body.get("detail", False)
                block_name = input_body.get("block_name", "")
		dataset = input_body.get("dataset", "") 
		release_version = input_body.get("release_version", "")
		pset_hash = input_body.get("pset_hash", "")
	        app_name = input_body.get("app_name", "")
		output_module_label = input_body.get("output_module_label", "")
		origin_site_name = input_body.get("origin_site_name", "")
		lumi_list = input_body.get("lumi_list", [])	
            except cjson.DecodeError as de:
                msg = "business/listFilss POST call requires at least dataset, block_name, or a list of logical_file_name %s" % de
                dbsExceptionHandler('dbsException-invalid-input', "Invalid input", self.logger.exception, msg)

        if ('%' in block_name):
            dbsExceptionHandler('dbsException-invalid-input', "You must specify exact block name not a pattern", self.logger.exception)
        elif ('%' in dataset):
	    print("***** in dataset name")
            dbsExceptionHandler('dbsException-invalid-input', " You must specify exact dataset name not a pattern", self.logger.exception)
        elif (not dataset  and not block_name and (not logical_file_name or '%'in logical_file_name) ):
            dbsExceptionHandler('dbsException-invalid-input', """You must specify one of the parameter groups:  \
                    non-pattern dataset, \
                    non-pattern block , non-pattern dataset with lfn ,\
                    non-pattern block with lfn or no-pattern lfn, \
		    non-patterned lfn list .""", self.logger.exception)
        elif (lumi_list and len(lumi_list) != 0):
            if run_num==-1:
                dbsExceptionHandler('dbsException-invalid-input', "Lumi list must accompany A single run number, \
                        use run_num=123", self.logger.exception)
            elif isinstance(run_num, str):
                try:
                    run_num = int(run_num)
                except:
                    dbsExceptionHandler('dbsException-invalid-input', "Lumi list must accompany A single run number,\
                        use run_num=123", self.logger.exception)
            elif isinstance(run_num, list):
                if len(run_num) == 1:
                    try:
                        run_num = int(run_num[0])
                    except:
                        dbsExceptionHandler('dbsException-invalid-input', "Lumi list must accompany A single run number,\
                            use run_num=123", self.logger.exception)
                else:
                    dbsExceptionHandler('dbsException-invalid-input', "Lumi list must accompany A single run number,\
                        use run_num=123", self.logger.exception)
	else:
            pass

        with self.dbi.connection() as conn:
            dao = (self.filebrieflist, self.filelist)[detail]
            for item in dao.execute(conn, dataset, block_name, logical_file_name, release_version, pset_hash, app_name,
                            output_module_label, run_num, origin_site_name, lumi_list, validFileOnly):

                yield item      # we need to yield while connection is open
    def insertFile(self, businput, qInserts=False):
        """
        This method supports bulk insert of files
        performing other operations such as setting Block and Dataset parentages,
        setting mapping between OutputConfigModules and File(s) etc.

        :param qInserts: True means that inserts will be queued instead of done immediately. INSERT QUEUE Manager will perform the inserts, within few minutes.
        :type qInserts: bool
        :param logical_file_name (required) : string
        :param is_file_valid: (optional, default = 1): 1/0
        :param block, required: /a/b/c#d
        :param dataset, required: /a/b/c
        :param file_type (optional, default = EDM): one of the predefined types,
        :param check_sum (optional): string
        :param event_count (optional, default = -1): int
        :param file_size (optional, default = -1.): float
        :param adler32 (optional): string
        :param md5 (optional): string
        :param auto_cross_section (optional, default = -1.): float
        :param file_lumi_list (optional, default = []): [{'run_num': 123, 'lumi_section_num': 12},{}....]
        :param file_parent_list(optional, default = []) :[{'file_parent_lfn': 'mylfn'},{}....]
        :param file_assoc_list(optional, default = []) :[{'file_parent_lfn': 'mylfn'},{}....]
        :param file_output_config_list(optional, default = []) :
        [{'app_name':..., 'release_version':..., 'pset_hash':...., output_module_label':...},{}.....]
        """

        # We do not want to go be beyond 10 files at a time
        # If user wants to insert over 10 files in one shot, we run into risks of locking the database
        # tables for longer time, and in case of error, it will be hard to see where error occured
        if len(businput) > 10:
            dbsExceptionHandler('dbsException-input-too-large', "DBS cannot insert \
                    more than 10 files in one bulk call")
            return

        conn = self.dbi.connection()
        tran = conn.begin()
        try:
            #Now we are dealing with independent files that have different dataset/block and so on.
            #See Trac #358.
            #The expected input data format is a list of dictionary to insert independent files into DBS,
            #inputdata={'files':[{}, {}, {}]}
            #YG  09/15/2011

            # AA- 01/06/2010 -- we have to do this file-by-file, there is no real good way to do this complex operation otherwise
            #files2insert = []
            #fidl = []
            fileInserted = False
            dataset = ""
            block_name = ""
            dataset_id = -1
            block_id = -1
            dsconfigs = []
            for f in businput:
                if not ("logical_file_name" in f and "block_name" in f and "dataset" in f ):
                    dbsExceptionHandler('dbsException-invalid-input', "DBSFile/insertFile must have logical_file_name, block_name and dataset as input")
                if f["block_name"].split('#')[0] != f["dataset"]:
                    dbsExceptionHandler('dbsException-invalid-input', "DBSFile/insertFile: dataset and block_name NOT match")
                # first check if the dataset exists
                # and block exists that files are suppose to be going to and is OPEN for writing
                if dataset != f["dataset"]:
                    dataset_id = self.datasetid.execute(conn, dataset=f["dataset"])
                    dataset = f["dataset"]
                    if dataset_id == -1 :
                        dbsExceptionHandler('dbsException-missing-data', "Required Dataset Not Found.", None,
                        "Required Dataset %s does not exist"%f["dataset"] )
                    # get the list of configs in for this dataset
                    dsconfigs = [x['output_mod_config_id'] for x in self.dsconfigids.execute(conn, dataset=f["dataset"])]
                fileconfigs = [] # this will hold file configs that we will list in the insert file logic below
                if block_name != f["block_name"]:
                    block_info = self.blocklist.execute(conn, block_name=f["block_name"])
		    for b in block_info:
			if not b  : 
			    dbsExceptionHandler( "dbsException-missing-data", "Required block not found", None,
                                                          "Cannot found required block %s in DB" %f["block_name"])
			else:	
			    if  b["open_for_writing"] != 1 : 
				dbsExceptionHandler("dbsException-conflict-data", "Block closed", None,
				    "Block %s is not open for writting" %f["block_name"])
			    if "block_id" in b:
				block_id = b["block_id"]
			    else:
				dbsExceptionHandler("dbsException-missing-data", "Block not found", None,
                                          "Cannot found required block %s in DB" %f["block_name"])
                else: dbsExceptionHandler('dbsException-missing-data', "Required block name Not Found in input.",
                                            None, "Required block Not Found in input.")
                #make the default file_type=EDM
                file_type_id = self.ftypeid.execute( conn, f.get("file_type", "EDM"))
                if file_type_id == -1:
                    dbsExceptionHandler('dbsException-missing-data', "File type not found.", None,
                                        "Required file type %s not found in DBS"%f.get("file_type", "EDM") )

                iFile = 0
                fileIncrement = 40
                fID = self.sm.increment(conn, "SEQ_FL", incCount=fileIncrement)
                #looping over the files, everytime create a new object 'filein' as you never know
                #whats in the original object and we do not want to know
                #for f in businput:
                file_clob = {}
                fparents2insert = []
                flumis2insert = []
                fconfigs2insert = []
                # create the file object from the original
                # taking care of defaults, and required
                filein = {
                    "logical_file_name" : f["logical_file_name"],
                    "is_file_valid" : f.get("is_file_valid", 1),
                    "check_sum" : f.get("check_sum", None),
                    "event_count" : f.get("event_count", -1),
                    "file_size" : f.get("file_size", -1),
                    "adler32" : f.get("adler32", None),
                    "md5" : f.get("md5", None),
                    "auto_cross_section" : f.get("auto_cross_section", -1),
                    #"creation_date" : f.get("creation_date", None),  See Ticket #965 YG.
                    #"create_by": f.get("create_by", None),
                    "last_modification_date": f.get("last_modification_date", None),
                    #"last_modified_by" : f.get("last_modified_by", None)
                    "last_modified_by" : dbsUtils().getCreateBy()
                }
                if filein["md5"] is None  and filein["check_sum"] is None and filein["adler32"] is None:
                    dbsExceptionHandler('dbsException-invalid-input', "Missing check_sum or adler32, or md5")
                if iFile == fileIncrement:
                    fID = self.sm.increment(conn, "SEQ_FL", incCount=fileIncrement)
                    iFile = 0
                filein["file_id"] = fID + iFile
                iFile += 1
                filein["dataset_id"] = dataset_id
                filein["block_id"] = block_id
                filein["file_type_id"] = file_type_id
                #FIXME: Add this later if f.get("branch_hash", "") not in ("", None):
                #filein["branch_hash"]=self.fbranchid.execute( f.get("branch_hash"), conn, transaction=tran)
                # insert file  -- as decided, one file at a time
                # filein will be what goes into database
                try:
                    if not qInserts:
                        self.filein.execute(conn, filein, transaction=tran)
                        fileInserted = True
                    else:
                        file_clob['file'] = filein
                except SQLAlchemyIntegrityError as ex:
                    if str(ex).find("unique constraint") != -1 or str(ex).lower().find("duplicate") != -1:
                        # Lets move on to NEXT file, we do not want to continue processing this file

                        #Nothing about this file is updated when it is already in DB. No file parentage, block parentage, dataset parentage and so on.
                        #Is this right?  YG  Oct. 24
                        self.logger.warning("DBSFile/insertFile. File already exists in DBS, not changing it: %s"
                                            %filein["logical_file_name"] )
                        continue
                    else:
                        raise

                #process file parents, file lumi, file outputmodconfigs, ...
                #file lumi sections
                if "file_lumi_list" in f:
                    fllist = f["file_lumi_list"]
                    if len(fllist) > 0:
                        for fl in fllist:
                            fldao = {
                                "run_num" : fl["run_num"],
                                "lumi_section_num" : fl["lumi_section_num"]
                            }
                            if "event_count" in fl:
                                fldao["event_count"] = fl["event_count"]
                            fldao["file_id"] = filein["file_id"]
                            flumis2insert.append(fldao)

                if "file_parent_list" in f:
                    #file parents
                    fplist = f["file_parent_list"]

                    for fp in fplist:
                        fpdao = {}
                        fpdao["this_file_id"] = filein["file_id"]
                        fpdao["parent_logical_file_name"] = fp["file_parent_lfn"]
                        fparents2insert.append(fpdao)
                if "file_output_config_list" in f:
                    #file output config modules
                    foutconfigs = f["file_output_config_list"]
                    if(len(foutconfigs) > 0):
                        for fc in foutconfigs:
                            fcdao = {}
                            fcdao["file_id"] = filein["file_id"]
                            fcdao["output_mod_config_id"] = self.outconfigid.execute(conn, fc["app_name"],
                                    fc["release_version"], fc["pset_hash"], fc["output_module_label"],
                                    fc["global_tag"])
                            if fcdao["output_mod_config_id"] == -1 :
                                dbsExceptionHandler('dbsException-missing-data', 'Config Not found.', None, "DBSFile/insertFile.\
                                        Output module config (%s, %s, %s, %s) \
                                        not found" % (fc["app_name"],
                                        fc["release_version"], fc["pset_hash"], fc["output_module_label"]) )
                            fileconfigs.append(fcdao["output_mod_config_id"])
                            fconfigs2insert.append(fcdao)
                #FIXME: file associations?-- in a later release
                #
                # insert file - lumi
                if flumis2insert:
                    file_clob['file_lumi_list'] = flumis2insert
                    if not qInserts:
                        self.flumiin.execute(conn, flumis2insert, transaction=tran)
                # insert file parent mapping
                if fparents2insert:
                    file_clob['file_parent_list'] = fparents2insert
                    if not qInserts:
                        self.fparentin.execute(conn, fparents2insert, transaction=tran)
                # First check to see if these output configs are mapped to THIS dataset as well, if not raise an exception
                if not set(fileconfigs).issubset(set(dsconfigs)) :
                    dbsExceptionHandler('dbsException-conflict-data', 'Mismatched configure. ', None, "DBSFile/insertFile. Output configs mismatch, \
                            output configs known to dataset: \
                            %s are different from what are being mapped to file : %s " \
                            %(f["dataset"], filein["logical_file_name"]) )
                # insert output module config mapping
                if fconfigs2insert:
                    file_clob['file_output_config_list'] = fconfigs2insert
                    if not qInserts:
                        self.fconfigin.execute(conn, fconfigs2insert, transaction=tran)
                if qInserts:
                    try:
                        self.logger.warning(file_clob)
                        self.filebufin.execute(conn, filein['logical_file_name'], block_id, file_clob, transaction=tran)
                    except SQLAlchemyIntegrityError as ex:
                        if str(ex).find("unique constraint") != -1 or str(ex).lower().find("duplicate") != -1:
                            pass
                        else:
                            raise

                #insert block parentages and dataset parentages based on file parentages
                # Do this one by one, as it is sure to have duplicate in dest table
                if fileInserted and fparents2insert:
                    for fp in fparents2insert:
                        try:
                            bkParentage2insert={'this_block_id' : filein["block_id"], 'parent_logical_file_name': fp['parent_logical_file_name']}
                            self.blkparentin.execute(conn, bkParentage2insert, transaction=tran)
                            dsParentage2insert={'this_dataset_id': filein["dataset_id"], 'parent_logical_file_name' : fp['parent_logical_file_name']}
                            self.dsparentin.execute(conn, dsParentage2insert, transaction=tran)
                        except SQLAlchemyIntegrityError as ex:
                            #ORA-00001
                            if (str(ex).find("ORA-00001") != -1 and str(ex).find("PK_DP") != -1) or str(ex).find("PK_BP") != -1 or str(ex).lower().find("duplicate") != -1:
                                pass
                            elif str(ex).find("ORA-01400") != -1:
                                raise
                            else:
                                raise

                # Update block parameters, file_count, block_size
                if not qInserts:
                    blkParams = self.blkstats.execute(conn, block_id,
                                                      transaction=tran)
                    blkParams['block_size'] = int(blkParams['block_size'])
                    self.blkstatsin.execute(conn, blkParams, transaction=tran)

            # All good ?
            tran.commit()
            tran = None

        except Exception as ex:
            if tran:
                tran.rollback()
                tran = None
            raise

        finally:
            if tran:
                tran.rollback()
            if conn:
                conn.close()
