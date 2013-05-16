#!/usr/bin/python
"""
DBS Exception class. 

"""

import exceptions
import inspect
import logging
import traceback

dbsExceptionCode = {
    'dbsException-db-conn-failed' : 'dbsException due to db connection failed: ',
    'dbsException-server-error' : 'dbsException due to internal server error: ',
    'dbsException-input-too-large' : "dbsException due to input exceeds a API's max limit : ",
    'dbsException-invalid-input' : 'dbsException due to invalid client input: ',
    'dbsException-invalid-input2' : 'dbsException due to invalid client input: ',
    'dbsException-missing-data' : 'dbsException required pre-existing data NOT in DBS: ',
    'dbsException-conflict-data' : 'dbsException due to data conflict between existing and input: '
}

class dbsException(exceptions.Exception):

    def __init__(self, eCode, message, serverError, **data):
        super(dbsException,self).__init__(message)
        self.name = str(self.__class__.__name__) 
        self.eCode=eCode
        self.message=message
        self.serverError=serverError
