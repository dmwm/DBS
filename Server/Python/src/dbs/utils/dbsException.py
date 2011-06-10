#!/usr/bin/python
"""
DBS Exception class. 

"""

import exceptions
import inspect
import logging
import traceback

dbsExceptionCode = {
    'dbsException-dao' : 'dbsException in dao: ',
    'dbsException-business' : 'dbsException in business: ',
    'dbsException-web' : 'dbsException in web: ',
    'dbsException-util' : 'dbsException in dbsUtil: ',
    'dbsException-migration' : 'dbsException in Migration: ',
    'dbsException-fileBuffer' : 'dbsException in FileBuffer: ',
    'dbsException-invalid-input' : 'dbsException due to invalid client input: ',
    'dbsException-invalid-input2' : 'dbsException due to invalid client input: ',
    'dbsException-missing-data' : 'dbsException required pre-existing data NOT in DBS: '
}

class dbsException(exceptions.Exception):

    def __init__(self, eCode, message, **data):
        self.name = str(self.__class__.__name__) 
        self.eCode=eCode
        self.message=message
