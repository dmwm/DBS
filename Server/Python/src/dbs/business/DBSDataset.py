#!/usr/bin/env python
"""
This module provides business object class to interact with Dataset. 
"""

__revision__ = "$Id: DBSDataset.py,v 1.7 2009/11/16 21:44:47 afaq Exp $"
__version__ = "$Revision: 1.7 $"

from WMCore.DAOFactory import DAOFactory

from exceptions import Exception

class DBSDataset:
    """
    Dataset business object class
    """
    def __init__(self, logger, dbi):
        """
        initialize business object class.
        """
        self.daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi)
        self.logger = logger
        self.dbi = dbi

    def listDatasets(self, dataset=""):
        """
        lists all datasets if dataset parameter is not given.
        The parameter can include % character. 
        """
        dao = self.daofactory(classname="Dataset.List")
        return dao.execute(dataset=dataset)

    def insertDataset(self, businput):
        """
        input dictionary must have the following keys:
        dataset, isdatasetvalid, primaryds(name), processedds(name), datatier(name),
        datasettype(name), acquisitionera(name), processingversion(name), 
        physicsgroup(name), xtcrosssection, globaltag, creationdate, createby, 
        lastmodificationdate, lastmodifiedby
        """ 
        seqmanager = self.daofactory(classname = "SequenceManager")
        classdict = {"primaryds":"PrimaryDataset.GetID",
                 "datatier":"DataTier.GetID",
                 "datasettype":"DatasetType.GetID",
                 #"acquisitionera":"AcquisitionEra.GetID",
                 #"processingversion":"ProcessingEra.GetID",
                 "physicsgroup":"PhysicsGroup.GetID"}
        
        conn = self.dbi.connection()
        tran = conn.begin()
        try:
            for k in classdict:
                dao = self.daofactory(classname = classdict[k])
                businput[k] = dao.execute(businput[k], conn, True)
            
            processedds = self.daofactory(classname = "ProcessedDataset.GetID")
	
            if processedds.execute(businput["processedds"]) > 0:
                businput["processedds"] = processedds.execute(businput["processedds"])
            else:
                dao = self.daofactory(classname = "ProcessedDataset.Insert")
                procid = seqmanager.increment("SEQ_PSDS", conn, True)
                procdaoinput = {"processeddsname":businput["processedds"],
                                    "processeddsid":procid}
                dao.execute(procdaoinput, conn, True)
                businput["processedds"] = procid
        
            businput["datasetid"] = seqmanager.increment("SEQ_DS", conn, True) 
            datasetinsert = self.daofactory(classname = "Dataset.Insert")
            datasetinsert.execute(businput, conn, True)
            tran.commit()
        except Exception, e:
            tran.rollback()
            self.logger.exception(e)
            raise
        finally:
            conn.close()
            
            
