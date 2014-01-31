from RestClient.RestApi import RestApi
from RestClient.AuthHandling.X509Auth import X509Auth
from RestClient.ProxyPlugins.Socks5Proxy import Socks5Proxy

import pycurl
import threading

class RestClientPool(object):
    _rest_client_pool = {}

    def __init__(self, auth_func=X509Auth, proxy=None):
        self._auth_func = auth_func
        self._proxy = Socks5Proxy(proxy_url=proxy) if proxy else None

    def get_rest_client(self):
        thread_id = threading.currentThread().ident
        try:
            return self._rest_client_pool[thread_id]
        except KeyError:
            self._rest_client_pool[thread_id] = RestApi(auth=self._auth_func(), proxy=self._proxy,
                                                        additional_curl_options={pycurl.NOSIGNAL: 1})
            ### see http://linux.die.net/man/3/libcurl-tutorial for thread safety
            return self._rest_client_pool[thread_id]

    def remove_rest_client(self):
        thread_id = threading.currentThread().ident
        try:
            del self._rest_client_pool[thread_id]
        except KeyError:
            pass
