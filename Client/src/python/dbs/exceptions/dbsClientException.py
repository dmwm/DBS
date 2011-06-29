"""
DBS Client Exception class
"""
import exceptions

class dbsClientException(exceptions.Exception):

    def __init__(self,reason,message):
        Exception.__init__(self)
        self.name = str(self.__class__.__name__)
        self.reason = reason
        self.message = message

    def __str__(self):
        return repr(self.reason+': '+self.message)
