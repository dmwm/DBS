#!/usr/bin/env python
"""
This module provides business object class to interact with Dataset. 
"""

__revision__ = "$Id: DBSDataset.py,v 1.4 2009/10/30 16:43:11 akhukhun Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.DAOFactory import DAOFactory

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

    def listDatasets(self, primdsname="", procdsname="", datatiername=""):
        """
        lists all datasets if none of the parameters are given.
        each parameter can include % character.
        """
        datasetlist = self.daofactory(classname="Dataset.List")
        result = datasetlist.execute(primdsname, procdsname, datatiername)
        return result

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
                 "acquisitionera":"AcquisitionEra.GetID",
                 "processingversion":"ProcessingEra.GetID",
                 "physicsgroup":"PhysicsGroup.GetID"}
        
        conn = self.dbi.connection()
        tran = conn.begin()
        try:
            for k in classdict.keys():
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
            
            