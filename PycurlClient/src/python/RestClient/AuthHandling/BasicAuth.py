from getpass import getpass
from RestClient.ErrorHandling.RestClientExceptions import ClientAuthException

import sys

class BasicAuth(object):
    def __init__(self, username=None, password=None):
        self._username = username
        self._password = password

        if sys.stdin.isatty() and not self._username:
            self._username = input('User:')

        if sys.stdin.isatty() and not self._password:
            self._password = getpass("Password:")

        if not (self._username and self._password):
            raise ClientAuthException("No valid user or password specified for BasicAuth")

    def configure_auth(self, curl_object):
        curl_object.setopt(curl_object.HTTPAUTH, curl_object.HTTPAUTH_BASIC)
        curl_object.setopt(curl_object.USERPWD, ("%s:%s") % (self.userpwd))

    @property
    def userpwd(self):
        return self._username, self._password
