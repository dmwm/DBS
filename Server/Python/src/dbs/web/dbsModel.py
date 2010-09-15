#
# $Revision: 1.1 $
# $Id: dbsModel.py,v 1.1 2009/10/06 22:01:38 afaq Exp $
# 
#  This is the class for primary dataset query objects.
#  
from WMCore.WebTools.RESTModel import RESTModel
# The business logic of all APIs is implemented in the business layer
from dbs.business import *

class dbsModel(RESTModel):
    """
    DBSModel class. Will write the documentation later. 	
    """

    def addService(self, verb, methodKey, func, args=[], validation=[], version=1):
        """
        add service (or any other method handler) in 
        """ 
        #TODO Wrap the function to the dict format (json)
        self.methods[verb][methodKey] = {'args': args,
                                         'call': func,
                                         'validation': [],
                                         'version': version}
        
        self.addService('GET', 'getwork', wq.getWork)
        self.addService('PUT', 'gotwork', wq.gotWork)
        self.addService('PUT', 'failwork', wq.failWork)


    def __init__(self, config):
        RESTModel.__init__(self, config)

        self.methods['GET']['listPrimaryDatasets'] = {'args':['primdsname'],'call': self.listPrimaryDatasets }

        self.methods['POST'] = {'create':{'args':['database', 'query'],
					'call':self.create}}
        self.methods['PUT']={'replace':{'args':['database', 'query'],
					'call':self.replace}}
        self.methods['DELETE']={'delete':{'args':['database', 'query'],
					  'call':self.delete}}

    ###
    #  Following provide the API inetrface to the DBS 
    # I do not like the close connection between REST-Model and the API here, lets see how it goes
    # The API should be a standalone interface, capabale of running standalone in Unit Tests !!!!!
    # So this architecture SUCKS !!
    ###

    def listPrimaryDatasets(self, args, kwargs):
	input=self.sanitise_input(args, kwargs)  
	return dbs.business.listPrimaryDatasets(input)





    def create(self, args, kwargs):
	data={'server_method':'create'}
	return data
    
    def replace(self, args, kwargs):
	data={'server_method':'replace'}
	return data

    def delete(self, args, kwargs):
	data={'server_method':'replace'}
	return data

