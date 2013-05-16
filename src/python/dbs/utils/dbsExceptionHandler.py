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
            #logger(eCode + ": " + serverError)
            raise HTTPError(400, message)
        elif eCode == "dbsException-missing-data":
            logger(eCode + ": " +  serverError)
            #print (eCode + ": " +  serverError)
            raise HTTPError(412, message)
        elif eCode == "dbsException-input-too-large":
            logger(eCode + ": " +  serverError)
            raise HTTPError(413, message)
        elif eCode == "dbsException-invalid-input2":
            logger(eCode + ": " +  serverError)
            raise HTTPError(400, message)
        elif eCode == "dbsException-conflict-data":
            logger(eCode + ": " +  serverError)
            raise HTTPError(409, message)
        else:
            #client gets httperror 500 for server internal error
            #print eCode + ": " +  serverError
            logger(eCode + ": " +  serverError)
            raise HTTPError(500, message)
    else:
        #not in the web layer
        raise dbsException(eCode, message, serverError)
