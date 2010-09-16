#!/usr/bin/env python
"""
This module provides business object class to interact with File. 
"""

__revision__ = "$Id: DBSFile.py,v 1.8 2009/11/27 09:55:03 akhukhun Exp $"
__version__ = "$Revision: 1.8 $"

from WMCore.DAOFactory import DAOFactory

class DBSFile:
    """
    File business object class
    """
    def __init__(self, logger, dbi, owner):
        self.daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi

        self.FileList = self.daofactory(classname="File.List")
        
        self.SequenceManager = self.daofactory(classname = "SequenceManager")
        self.FileInsert = self.daofactory(classname = "File.Insert")
        self.FileLumiInsert = self.daofactory(classname = "FileLumi.Insert")
        self.FileParentInsert = self.daofactory(classname = "FileParent.Insert")
        self.FileGetID = self.daofactory(classname = "File.GetID")

        self.DatasetGetID = self.daofactory(classname = "Dataset.GetID")
        self.BlockGetID = self.daofactory(classname = "Block.GetID")
        self.FileTypeGetID = self.daofactory(classname = "FileType.GetID")

    def listFiles(self, dataset = "", block = "", lfn = ""):
        """
        returns all files in a datasest if block and lfn pattern
        are not specified
        """
        return self.FileList.execute(dataset, block, lfn)


    def insertFile(self, businput):
        """
        businput is a list of dictionaries with the following keys:
        fix the keys:
        """

        #need to get the ID's of following keys
        classdict = {"DATASET":"Dataset.GetID",
                     "BLOCK":"Block.GetID",
                     "FILE_TYPE":"FileType.GetID"}
        conn = self.dbi.connection()
        tran = conn.begin()
        try:
            files2insert = []
            fparents2insert = []
            flumis2insert = []
    

            #get ID's from values only for the first file.
            #Others will probably be the same
            firstfile = businput[0]
            firstfile["DATASET"] = self.DatasetGetID.execute(firstfile["DATASET"], conn, True)
            firstfile["BLOCK"] = self.BlockGetID.execute(firstfile["BLOCK"], conn, True)
            firstfile["FILE_TYPE"] = self.FileTypeGetID.execute(firstfile["FILE_TYPE"], conn, True)
                
            iFile = 0
            fileIncrement = 40
            fID = self.SequenceManager.increment("SEQ_FL", conn, True) 
            #looping over the files
            for f in businput:
                if iFile == fileIncrement:
                    fID = self.SequenceManager.increment("SEQ_FL", conn, True) 
                    iFile = 0
                f["FILE_ID"] = fID + iFile
                iFile += 1
                    
                for k in classdict: 
                    f[k] = firstfile[k]
                    
                #insert file
                f2insert = f.copy()
                f2insert.pop("FILE_LUMI_LIST")
                f2insert.pop("FILE_PARENT_LIST")
                files2insert.append(f2insert)
            
                #isnert file lumi sections
                fllist = f["FILE_LUMI_LIST"]
                if(len(fllist) > 0):
                    iLumi = 0
                    flIncrement = 1000
                    flID = self.SequenceManager.increment("SEQ_FLM", conn, True)
                    for fl in fllist:
                        if iLumi == flIncrement:
                            flID =  self.SequenceManager.increment("SEQ_FLM", conn, True)
                            iLumi = 0
                        fl["FILE_LUMI_ID"] = flID + iLumi
                        iLumi += 1
                        fl["FILE_ID"] = f["FILE_ID"]
                    flumis2insert.extend(fllist)
            
                #insert file parents    
                fplist = f["FILE_PARENT_LIST"]
                if(len(fplist) > 0):
                    iParent = 0
                    fpIncrement = 120
                    fpID = self.SequenceManager.increment("SEQ_FP", conn, True)
                    
                    for fp in fplist:
                        if iParent == fpIncrement:
                            fpID = self.SequenceManager.increment("SEQ_FP", conn, True)
                            iParent  = 0
                        fp["FILE_PARENT_ID"] = fpID + iParent
                        iParent += 1 
                        fp["THIS_FILE_ID"] = f["FILE_ID"]
                        lfn = fp["FILE_PARENT_LFN"]
                        fp["FILE_PARENT_LFN"] = self.FileGetID.execute(lfn, conn, True)
                    fparents2insert.extend(fplist)

            self.FileInsert.execute(files2insert, conn, True)
            if flumis2insert:
                self.FileLumiInsert.execute(flumis2insert, conn, True)
            if fparents2insert:
                self.FileParentInsert.execute(fparents2insert, conn, True)
            tran.commit()

        except Exception, e:
            tran.rollback()
            self.logger.exception(e)
            raise
        finally:
            conn.close()
