#!/usr/bin/env python
"""
This module provides business object class to interact with File. 
"""

__revision__ = "$Id: DBSFile.py,v 1.2 2009/11/12 15:19:35 akhukhun Exp $"
__version__ = "$Revision: 1.2 $"

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
        """
        seqmanager = self.daofactory(classname = "SequenceManager")
        #need to get the ID's of following keys
        classdict = {"dataset":"Dataset.GetID",
                     "block":"Block.GetID",
                     "filetype":"FileType.GetID",
                     "branchhash":"BranchHashe.GetID"}
        conn = self.dbi.connection()
        tran = conn.begin()
        try:
            #get ID's from values only for the first file.
            #Others will probably be the same
            firstfile = businput[0]
            for k in classdict:
                dao = self.daofactory(classname = classdict[k])
                firstfile[k] = dao.execute(firstfile[k], conn, True)
            fileids = seqmanager.incrementN("SEQ_FL", len(businput), conn, True)
            for i in range(len(businput)):
                f = businput[i]
                f["fileid"] = fileids[i] 
                for k in classdict: f[k] = firstfile[k]
            fileinsert = self.daofactory(classname = "File.Insert")
            fileinsert.execute(businput, conn, True)
            tran.commit()
            
        except Exception, e:
            tran.rollback()
            self.logger.exception(e)
            raise
        finally:
            conn.close()
                
                    
                
            
