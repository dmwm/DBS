"""
DBS 3 Hello World REST Example
"""
from WMCore.REST.Server import RESTEntity, restcall
from WMCore.REST.Tools import tools
from WMCore.REST.Validation import *

from dbs.utils.Validation import string_validation_rx

class HelloWorld(RESTEntity):
    def validate(self, apiobj, method, api, param, safe):
        """
        Validate input data
        """
        validate_str("name", param, safe, string_validation_rx, optional=True)

    @restcall
    def get(self, name):
        """
        DBS3 Hello World API
        """
        msg = "Hello World"

        if name:
            msg += " from %s" % name

        return msg
