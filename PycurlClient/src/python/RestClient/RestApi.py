from RestClient.RequestHandling.HTTPRequest import HTTPRequest

import pycurl

class RestApi(object):
    def __init__(self, auth=None, proxy=None, additional_curl_options=None, use_shared_handle=False):
        self.curl_pool = []
        self.use_shared_handle = use_shared_handle
        self.add_curl_options = additional_curl_options if additional_curl_options else {}
        self.proxy = proxy
        self.auth = auth
        self.curl_pool(self.newCurl())

    def newCurl(self):
        "Create new curl object, sets its options and returns it back to the caller"
        curl = pycurl.Curl()

        if self.use_shared_handle:
            ###use shared Cookie, DNS and SSL Session ID caches when operating multi-threaded
            shared_curl = pycurl.CurlShare()
            shared_curl.setopt(pycurl.SH_SHARE, pycurl.LOCK_DATA_COOKIE)
            shared_curl.setopt(pycurl.SH_SHARE, pycurl.LOCK_DATA_DNS)
            shared_curl.setopt(pycurl.SH_SHARE, pycurl.LOCK_DATA_SSL_SESSION)
            curl.setopt(pycurl.SHARE, shared_curl)

        if self.auth:
            self.auth.configure_auth(curl)
        if self.proxy:
            self.proxy.configure_proxy(curl)
        return curl

    def getCurl(self):
        "Fetch one curl object form the pool or create a new one"
        if len(self.curl_pool):
            curl = self.curl_pool[-1]
            self.curl_pool = self.curl_pool[:-1]
        else:
            curl = self.newCurl()
        return curl

    def __del__(self):
        "Perform clean-up procedure: close all curl connections"
        for curl in self.curl_pool:
            curl.close()

    def execute(self, http_request):
        "Execute given http_request with available curl instance"
        curl = self.getCurl()
        res = http_request(curl)
        self.curl_pool.append(curl)
        return res

    def get(self, url, api, params=None, data=None, request_headers=None):
        "Perform get HTTP request for given set of parameters"
        if not params:
            params = {}
        if not request_headers:
            request_headers = {}
        http_request = HTTPRequest(method='GET', url=url, api=api, params=params,
                                   data=data, request_headers=request_headers,
                                   additional_curl_options=self.add_curl_options)
        return self.execute(http_request)

    def post(self, url, api, params=None, data="", request_headers=None):
        "Perform postt HTTP request for given set of parameters"
        if not params:
            params = {}
        if not request_headers:
            request_headers = {}
        http_request = HTTPRequest(method='POST', url=url, api=api, params=params, data=data,
                                   request_headers=request_headers,
                                   additional_curl_options=self.add_curl_options)
        return self.execute(http_request)

    def put(self, url, api, params=None, data="", request_headers=None):
        "Perform put HTTP request for given set of parameters"
        if not params:
            params = {}
        if not request_headers:
            request_headers = {}
        http_request = HTTPRequest(method='PUT', url=url, api=api, params=params, data=data,
                                   request_headers=request_headers,
                                   additional_curl_options=self.add_curl_options)
        return self.execute(http_request)

    def delete(self, url, api, params=None, data=None, request_headers=None):
        "Perform delete HTTP request for given set of parameters"
        if not params:
            params = {}
        if not request_headers:
            request_headers = {}
        http_request = HTTPRequest(method='DELETE', url=url, api=api, params=params, data=data,
                                   request_headers=request_headers,
                                   additional_curl_options=self.add_curl_options)
        return self.execute(http_request)
