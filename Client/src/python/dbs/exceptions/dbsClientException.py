"""
DBS Client Exception class
"""
try:
    from exceptions import Exception
except ImportError:
    # Assuming we are running under python3 environment:
    pass


class dbsClientException(Exception):

    def __init__(self, reason, message):
        Exception.__init__(self)
        self.name = str(self.__class__.__name__)
        self.reason = reason
        self.message = message

    def __str__(self):
        return repr(self.reason+': '+self.message)
