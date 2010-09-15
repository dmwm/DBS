#!/usr/bin/env python
"""
This module provides business object class to interact with Dataset. 
"""

__revision__ = "$Id: DBSDataset.py,v 1.9 2009/11/27 09:55:02 akhukhun Exp $"
__version__ = "$Revision: 1.9 $"

from WMCore.DAOFactory import DAOFactory

from exceptions import Exception

class DBSDataset:
    """
    Dataset business object class
    """
    def __init__(self, logger, dbi, owner):
        """
        initialize business object class.
        """
        self.daofactory = DAOFactory(package='dbs.dao', logger=logger, dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        
        self.DatasetList = self.daofactory(classname="Dataset.List")

        self.SequenceManager = self.daofactory(classname="SequenceManager")
        self.PrimaryDatasetGetID = self.daofactory(classname='PrimaryDataset.GetID')
        self.DataTierGetID = self.daofactory(classname='DataTier.GetID')
        self.DatasetTypeGetID = self.daofactory(classname='DatasetType.GetID')
        self.PhysicsGroupGetID = self.daofactory(classname='PhysicsGroup.GetID')
        self.ProcessedDatasetGetID = self.daofactory(classname='ProcessedDataset.GetID')
        self.ProcessedDatasetInsert = self.daofactory(classname='ProcessedDataset.Insert')
        self.DatasetInsert = self.daofactory(classname='Dataset.Insert')


    def listDatasets(self, dataset=""):
        """
        lists all datasets if dataset parameter is not given.
        The parameter can include % character. 
        """
        return self.DatasetList.execute(dataset=dataset)

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
                 "physicsgroup":"PhysicsGroup.GetID"}
        
        conn = self.dbi.connection()
        tran = conn.begin()
        try:
            businput["primaryds"] = self.PrimaryDatasetGetID.execute(businput["primaryds"], conn, True)
            businput["datatier"] = self.DataTierGetID.execute(businput["datatier"], conn, True)
            businput["datasettype"] = self.DatasetTypeGetID.execute(businput["datasettype"], conn, True)
            businput["physicsgroup"] = self.PhysicsGroupGetID.execute(businput["physicsgroup"], conn, True)

            procid = self.ProcessedDatasetGetID.execute(businput["processedds"])
            if procid > 0:
                businput["processedds"] = procid
            else:
                procid = self.SequenceManager.increment("SEQ_PSDS", conn, True)
                procdaoinput = {"processeddsname":businput["processedds"],
                                    "processeddsid":procid}
                self.ProcessedDatasetInsert.execute(procdaoinput, conn, True)
                businput["processedds"] = procid
        
            businput["datasetid"] = self.SequenceManager.increment("SEQ_DS", conn, True) 
            self.DatasetInsert.execute(businput, conn, True)
            tran.commit()
        except Exception, e:
            tran.rollback()
            self.logger.exception(e)
            raise
        finally:
            conn.close()
            
            
