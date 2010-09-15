#!/usr/bin/env python
"""
This module provides business object class to interact with Dataset. 
"""

__revision__ = "$Id: DBSDataset.py,v 1.10 2009/11/30 09:52:31 akhukhun Exp $"
__version__ = "$Revision: 1.10 $"

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
        daofactory = DAOFactory(package='dbs.dao', logger=logger, 
                                dbinterface=dbi, owner=owner)
        self.logger = logger
        self.dbi = dbi
        
        self.datasetlist = daofactory(classname="Dataset.List")
        self.sm = daofactory(classname="SequenceManager")
        self.primdsid = daofactory(classname='PrimaryDataset.GetID')
        self.tierid = daofactory(classname='DataTier.GetID')
        self.datatypeid = daofactory(classname='DatasetType.GetID')
        self.phygrpid = daofactory(classname='PhysicsGroup.GetID')
        self.procdsid = daofactory(classname='ProcessedDataset.GetID')
        self.procdsin = daofactory(classname='ProcessedDataset.Insert')
        self.datasetin = daofactory(classname='Dataset.Insert')


    def listDatasets(self, dataset=""):
        """
        lists all datasets if dataset parameter is not given.
        The parameter can include % character. 
        """
        return self.datasetlist.execute(dataset=dataset)
    
    
    def insertDataset(self, businput):
        """
        input dictionary must have the following keys:
        dataset, isdatasetvalid, primaryds(name), processedds(name), datatier(name),
        datasettype(name), acquisitionera(name), processingversion(name), 
        physicsgroup(name), xtcrosssection, globaltag, creationdate, createby, 
        lastmodificationdate, lastmodifiedby
        """ 
        conn = self.dbi.connection()
        tran = conn.begin()
        try:
            businput["primaryds"] = self.primdsid.execute(businput["primaryds"], conn, True)
            businput["datatier"] = self.tierid.execute(businput["datatier"], conn, True)
            businput["datasettype"] = self.datatypeid.execute(businput["datasettype"], conn, True)
            businput["physicsgroup"] = self.phygrpid.execute(businput["physicsgroup"], conn, True)

            procid = self.procdsid.execute(businput["processedds"])
            if procid > 0:
                businput["processedds"] = procid
            else:
                procid = self.sm.increment("SEQ_PSDS", conn, True)
                procdaoinput = {"processeddsname":businput["processedds"],
                                    "processeddsid":procid}
                self.procdsin.execute(procdaoinput, conn, True)
                businput["processedds"] = procid
        
            businput["datasetid"] = self.sm.increment("SEQ_DS", conn, True) 
            self.datasetin.execute(businput, conn, True)
            tran.commit()
        except Exception, e:
            tran.rollback()
            self.logger.exception(e)
            raise
        finally:
            conn.close()
            
            
