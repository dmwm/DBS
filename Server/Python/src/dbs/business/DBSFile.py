#!/usr/bin/env python
"""
This module provides business object class to interact with File. 
"""

__revision__ = "$Id: DBSFile.py,v 1.3 2009/11/16 19:23:40 yuyi Exp $"
__version__ = "$Revision: 1.3 $"

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
        return dao.execute(dataset, block, lfn)

    def insertFile(self, businput):
        """
        businput is a list of dictionaries with the following keys:
        logicalfilename, isfilevalid, dataset(/a/b/c), block(/a/b/c#d), 
        filetype(type), checksum, eventcount, filesize, branchhash(hash), 
        adler32, md5, autocrosssection, creationdate, createby,
        lastmodificationdate, lastmodifiedby
	
	We are trying to insert file lumi and filre parents. YG 11/16/09
        """
        seqmanager = self.daofactory(classname = "SequenceManager")
        #need to get the ID's of following keys
        classdict = {"DATASET":"Dataset.GetID",
                     "BLOCK":"Block.GetID",
                     "FILE_TYPE":"FileType.GetID"}
        conn = self.dbi.connection()
        tran = conn.begin()
        try:
            #get ID's from values only for the first file.
            #Others will probably be the same
	    
	    # We assume all the files to be inserted has the same type, dataset
	    # and block? Right, Aleko?  11/16/09 YG
	    #
            firstfile = businput[0]
            for k in classdict:
                dao = self.daofactory(classname = classdict[k])
                firstfile[k] = dao.execute(firstfile[k], conn, True)
            fileids = seqmanager.incrementN("SEQ_FL", len(businput), conn, True)
            for i in range(len(businput)):
                f = businput[i]
                f["FILE_ID"] = fileids[i] 
                for k in classdict: f[k] = firstfile[k]
		    """ 
		    Talked with Steve F. It seems that WMCore cannot handle
		    parameters binding with a dictionary that has more keys than
		    what the sql request in the dao object. So one has to
		    physically copy the input direction into a new one with exactly 
		    same number of binding variables.
		    """
		    file2inster = f.copy()
		    file2insert.pop("FILE_LUMI_LIST")
		    file2insert.pop("FILE_PARENT_LIST")
		    fileinsert = self.daofactory(classname = "File.Insert")
		    fileinsert.execute(file2insert, conn, True)
		    print  "Done insert file: " + file2insert["LOGICAL_FILE_NAME"];
		    #
		    lumi2insert = f["FILE_LUMI_LIST"]
		    lumiIds = seqmanager.incrementN("SEQ_FLM", len(lumi2insert), conn,
				    True)
		    for j in range(len(lumi2insert)):
			lumi2insert[j]["FILE_LUMI_ID"] = lumiIds[j]
			lumi2insert[j]["FILE_ID"] = f["FILE_ID"]
		    fileLumiDao = self.daofactory(classname = "FileLumi.Insert")
		    fileLumiDao.execute(lumi2insert, conn, True)
		    print "Done bulk insert for file lumi"
		    #
		    parent2insert = f["FILE_PARENT_LIST"]
		    fileParentIds = seqmanager.incrementN("SEQ_FP", len(parent2insert), conn,
		                                        True)
		    for m in range(len(parent2insert)):
			parent2insert[m]["FILE_PARENT_ID"] = fileParentIds[m]
			parent2insert[m]["THIS_FILE_ID"] = f["FILE_ID"]
			fileDao = self.daofactory(classname ="File.List")
			output=fileDao.execute(dataset = "", block = "",
			    lfn=parent2insert[m][FILE_PARENT_LFN], conn, True)
			parent2insert[m]["FILE_PARENT_LFN"] = output[0]["FILE_ID"]
		    fileParentDao = self.daofactory(classname = "FileParent.Insert")
		    fileParentDao.execute(parent2insert, conn, True)
		    print "Done Bulk insert for file parentage"
		    tran.commit()
        except Exception, e:
            tran.rollback()
            self.logger.exception(e)
            raise
        finally:
            conn.close()
                
                    
                
            
