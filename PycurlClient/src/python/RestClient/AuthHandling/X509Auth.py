from getpass import getpass
from RestClient.ErrorHandling.RestClientExceptions import ClientAuthException

import os, sys

class X509Auth(object):
    def __init__(self, ca_path=None, ssl_cert=None, ssl_key=None):
        self._ca_path = ca_path
        self._ssl_cert = ssl_cert
        self._ssl_key = ssl_key

        if not (self._ssl_cert and self._ssl_key):
            self.__search_cert_key()

        if not self._ca_path:
            self.__search_ca_path()

        #Check if ssl_cert, ssl_key and ca_path do exist
        if not (os.path.isfile(self._ssl_key) and os.path.isfile(self._ssl_cert)):
            raise ClientAuthException("key or cert file does not exist: %s, %s" % (self._ssl_key,self._ssl_cert))

        if not (os.path.isdir(self._ca_path)):
            raise ClientAuthException("CA path does not exist: %s" % (self._ca_path))

    def __search_ca_path(self):
        """
        Get CA Path to check the validity of the server host certificate on the client side
        """
        if os.environ.has_key("X509_CERT_DIR"):
            self._ca_path = os.environ['X509_CERT_DIR']

        elif os.path.exists('/etc/grid-security/certificates'):
            self._ca_path = '/etc/grid-security/certificates'

        else:
            raise ClientAuthException("Could not find a valid CA path")
    
    def __search_cert_key(self):
        """
        Get the user credentials if they exist, otherwise throw an exception.
        This code was modified from DBSAPI/dbsHttpService.py and WMCore/Services/Requests.py
        """
        # Now we're trying to guess what the right cert/key combo is...
        # First preference to HOST Certificate, This is how it set in Tier0
        if os.environ.has_key('X509_HOST_CERT'):
            self._ssl_cert = os.environ['X509_HOST_CERT']
            self._ssl_key = os.environ['X509_HOST_KEY']

        # Second preference to User Proxy, very common
        elif os.environ.has_key('X509_USER_PROXY') and os.path.exists(os.environ['X509_USER_PROXY']):
            self._ssl_cert = os.environ['X509_USER_PROXY']
            self._ssl_key = self._ssl_cert

        # Third preference to User Cert/Proxy combinition
        elif os.environ.has_key('X509_USER_CERT') and os.environ.has_key('X509_USER_KEY'):
            self._ssl_cert = os.environ['X509_USER_CERT']
            self._ssl_key = os.environ['X509_USER_KEY']

        # TODO: only in linux, unix case, add other os case
        # look for proxy at default location /tmp/x509up_u$uid
        elif os.path.exists(os.path.join('/tmp/x509up_u', str(os.getuid()))):
            self._ssl_cert = os.path.join('/tmp/x509up_u', str(os.getuid()))
            self._ssl_key = self._ssl_cert

        elif sys.stdin.isatty():
            home_dir = os.environ['HOME']
            user_cert = os.path.join(home_dir, '.globus/usercert.pem')
            user_key = os.path.join(home_dir, '.globus/userkey.pem')

            if os.path.exists(user_cert):
                self._ssl_cert = user_cert
                if os.path.exists(user_key):
                    self._ssl_key = user_key
                    #store password for convenience
                    self._ssl_key_pass = getpass("Password for %s: " % self._ssl_key)
                else:
                    self._ssl_key = self._ssl_cert
            else:
                raise ClientAuthException("No valid X509 cert-key-pair found.")    

        else:
            raise ClientAuthException("No valid X509 cert-key-pair found.")

    def configure_auth(self, curl_object):
        curl_object.setopt(curl_object.CAPATH, self._ca_path)
        curl_object.setopt(curl_object.SSLCERT, self._ssl_cert)
        curl_object.setopt(curl_object.SSLKEY, self._ssl_key)

        if self.ssl_key_pass:
            curl_object.setopt(curl_object.SSLKEYPASSWD, self.ssl_key_pass)

    @property
    def ssl_key_pass(self):
        return getattr(self, '_ssl_key_pass', None)
