from cherrypy import request, response, HTTPError
from dbs.utils.dbsException import dbsException,dbsExceptionCode
def dbsExceptionHandler(eCode='', message='', logger=None , serverError=''):
    """
    This utility function handles all dbs exceptions. It will log , raise exception
    based on input condition. It loggs the traceback on the server log. Send HTTPError 400 
    for invalid client input and HTTPError 404 for NOT FOUND required pre-existing condition. 
    """
    if  logger:
        #at the web layer
        if eCode == "dbsException-invalid-input":
            raise HTTPError(400, message)
        elif eCode == "dbsException-missing-data":
            logger(eCode + ": " +  serverError)
            #print (eCode + ": " +  serverError)
            raise HTTPError(404, message)
        elif eCode == "dbsException-invalid-input2":
            logger(eCode + ": " +  serverError)
            raise HTTPError(404, message)
        else:
            #client gets httperror 500 for server internal error
            #print eCode + ": " +  serverError
            logger(eCode + ": " +  serverError)
            raise dbsException(eCode, message)
    else:
        #not in the web layer
        raise dbsException(eCode, message)
        
        
