#!/usr/bin/env python
"""
This module provides business object class to interact with File. 
"""

__revision__ = "$Id: DBSFile.py,v 1.6 2009/11/19 17:32:10 akhukhun Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.DAOFactory import DAOFactory

class DBSFile:
    """
    File business object class
    """
    def __init__(self, logger, dbi):
        self.daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi)
        self.logger = logger
        self.dbi = dbi

    def listFiles(self, dataset = "", block = "", lfn = ""):
        """
        returns all files in a datasest if block and lfn pattern
        are not specified
        """
        dao = self.daofactory(classname="File.List")
        #should we use our own connection with transation=True ???
        return dao.execute(dataset, block, lfn)

    def insertFile(self, businput):
        """
        businput is a list of dictionaries with the following keys:
        fix the keys:
        """
        seqmanager = self.daofactory(classname = "SequenceManager")
        fileinsert = self.daofactory(classname = "File.Insert")
        flinsert = self.daofactory(classname = "FileLumi.Insert")
        fpinsert = self.daofactory(classname = "FileParent.Insert")
        fileGetID = self.daofactory(classname = "File.GetID")

        #need to get the ID's of following keys
        classdict = {"DATASET":"Dataset.GetID",
                     "BLOCK":"Block.GetID",
                     "FILE_TYPE":"FileType.GetID"}
        conn = self.dbi.connection()
        tran = conn.begin()
        try:
            #get ID's from values only for the first file.
            #Others will probably be the same
            firstfile = businput[0]
            for k in classdict:
                dao = self.daofactory(classname = classdict[k])
                firstfile[k] = dao.execute(firstfile[k], conn, True)
                
            iFile = 0
            fileIncrement = 40
            fID = seqmanager.increment("SEQ_FL", conn, True) 
            #looping over the files
            for f in businput:
                if iFile == fileIncrement:
                    fID = seqmanager.increment("SEQ_FL", conn, True) 
                    iFile = 0
                f["FILE_ID"] = fID + iFile
                iFile += 1
                    
                for k in classdict: 
                    f[k] = firstfile[k]
                    
                #insert file
                file2insert = f.copy()
                file2insert.pop("FILE_LUMI_LIST")
                file2insert.pop("FILE_PARENT_LIST")
                fileinsert.execute(file2insert, conn, True)
            
                #isnert file lumi sections
                fllist = f["FILE_LUMI_LIST"]
                if(len(fllist) > 0):
                    iLumi = 0
                    flIncrement = 1000
                    flID = seqmanager.increment("SEQ_FLM", conn, True)
                    for fl in fllist:
                        if iLumi == flIncrement:
                            flID =  seqmanager.increment("SEQ_FLM", conn, True)
                            iLumi = 0
                        fl["FILE_LUMI_ID"] = flID + iLumi
                        iLumi += 1
                        fl["FILE_ID"] = f["FILE_ID"]
                    flinsert.execute(fllist, conn, True)
            
                #insert file parents    
                fplist = f["FILE_PARENT_LIST"]
                if(len(fplist) > 0):
                    iParent = 0
                    fpIncrement = 120
                    fpID = seqmanager.increment("SEQ_FP", conn, True)
                    
                    for fp in fplist:
                        if iParent == fpIncrement:
                            fpID = seqmanager.increment("SEQ_FP", conn, True)
                            iParent  = 0
                        fp["FILE_PARENT_ID"] = fpID + iParent
                        iParent += 1 
                        fp["THIS_FILE_ID"] = f["FILE_ID"]
                        lfn = fp["FILE_PARENT_LFN"]
                        fp["FILE_PARENT_LFN"] = fileGetID.execute(lfn, conn, True)
                    fpinsert.execute(fplist, conn, True)
                
            tran.commit()

        except Exception, e:
            tran.rollback()
            self.logger.exception(e)
            raise
        finally:
            conn.close()
                
                    
                
            
