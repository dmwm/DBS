#!/usr/bin/env python
"""
This module provides business object class to interact with File. 
"""

__revision__ = "$Id: DBSFile.py,v 1.10 2009/12/07 15:46:06 akhukhun Exp $"
__version__ = "$Revision: 1.10 $"

from WMCore.DAOFactory import DAOFactory

class DBSFile:
    """
    File business object class
    """
    def __init__(self, logger, dbi, owner):
        daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi

        self.filelist = daofactory(classname="File.List")
        self.sm = daofactory(classname = "SequenceManager")
        self.filein = daofactory(classname = "File.Insert")
        self.flumiin = daofactory(classname = "FileLumi.Insert")
        self.fparentin = daofactory(classname = "FileParent.Insert")
        self.fileid = daofactory(classname = "File.GetID")
        self.datasetid = daofactory(classname = "Dataset.GetID")
        self.blockid = daofactory(classname = "Block.GetID")
        self.ftypeid = daofactory(classname = "FileType.GetID")


    def listFiles(self, dataset = "", block = "", lfn = ""):
        """
        either dataset(and lfn pattern) or block(and lfn pattern) must be specified.
        """
        conn = self.dbi.connection()
        result = self.filelist.execute(dataset, block, lfn, conn)
        conn.close()
        return result


    def insertFile(self, businput):
        """
        This method inserts files. Supports bulk insert.
        'businput' must be the list of dictionaries.
        """

        #need to get the ID's of following keys
        conn = self.dbi.connection()
        tran = conn.begin()
        try:
            files2insert = []
            fparents2insert = []
            flumis2insert = []
    
            #get dataset, block and file-type id's only for the first file.
            firstfile = businput[0]
            firstfile["DATASET"] = self.datasetid.execute(firstfile["DATASET"], conn, True)
            firstfile["BLOCK"] = self.blockid.execute(firstfile["BLOCK"], conn, True)
            firstfile["FILE_TYPE"] = self.ftypeid.execute(firstfile["FILE_TYPE"], conn, True)
                
            iFile = 0
            fileIncrement = 40
            fID = self.sm.increment("SEQ_FL", conn, True) 
            #looping over the files
            for f in businput:
                if iFile == fileIncrement:
                    fID = self.sm.increment("SEQ_FL", conn, True) 
                    iFile = 0
                f["FILE_ID"] = fID + iFile
                iFile += 1
                    
                f["DATASET"] = firstfile["DATASET"]
                f["BLOCK"] = firstfile["BLOCK"]
                f["FILE_TYPE"] = firstfile["FILE_TYPE"]
                    
                #file
                f2insert = f.copy()
                f2insert.pop("FILE_LUMI_LIST")
                f2insert.pop("FILE_PARENT_LIST")
                files2insert.append(f2insert)
            
                #file lumi sections
                fllist = f["FILE_LUMI_LIST"]
                if(len(fllist) > 0):
                    iLumi = 0
                    flIncrement = 1000
                    flID = self.sm.increment("SEQ_FLM", conn, True)
                    for fl in fllist:
                        if iLumi == flIncrement:
                            flID =  self.sm.increment("SEQ_FLM", conn, True)
                            iLumi = 0
                        fl["FILE_LUMI_ID"] = flID + iLumi
                        iLumi += 1
                        fl["FILE_ID"] = f["FILE_ID"]
                    flumis2insert.extend(fllist)
            
                #file parents    
                fplist = f["FILE_PARENT_LIST"]
                if(len(fplist) > 0):
                    iParent = 0
                    fpIncrement = 120
                    fpID = self.sm.increment("SEQ_FP", conn, True)
                    
                    for fp in fplist:
                        if iParent == fpIncrement:
                            fpID = self.sm.increment("SEQ_FP", conn, True)
                            iParent  = 0
                        fp["FILE_PARENT_ID"] = fpID + iParent
                        iParent += 1 
                        fp["THIS_FILE_ID"] = f["FILE_ID"]
                        lfn = fp["FILE_PARENT_LFN"]
                        fp["FILE_PARENT_LFN"] = self.fileid.execute(lfn, conn, True)
                    fparents2insert.extend(fplist)

            self.filein.execute(files2insert, conn, True)
            if flumis2insert:
                self.flumiin.execute(flumis2insert, conn, True)
            if fparents2insert:
                self.fparentin.execute(fparents2insert, conn, True)
            tran.commit()

        except Exception, e:
            tran.rollback()
            self.logger.exception(e)
            raise
        finally:
            conn.close()
