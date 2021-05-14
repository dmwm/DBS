from RestClient.RequestHandling.HTTPRequest import HTTPRequest
import pycurl


class RestApi(object):
    def __init__(self, auth=None, proxy=None, additional_curl_options=None, use_shared_handle=False):
        self._curl = pycurl.Curl()

        if use_shared_handle:
            ###use shared Cookie, DNS and SSL Session ID caches when operating multi-threaded
            shared_curl = pycurl.CurlShare()
            shared_curl.setopt(pycurl.SH_SHARE, pycurl.LOCK_DATA_COOKIE)
            shared_curl.setopt(pycurl.SH_SHARE, pycurl.LOCK_DATA_DNS)
            shared_curl.setopt(pycurl.SH_SHARE, pycurl.LOCK_DATA_SSL_SESSION)
            self._curl.setopt(pycurl.SHARE, shared_curl)

        if additional_curl_options:
            self._additional_curl_options = additional_curl_options
        else:
            self._additional_curl_options = {}

        if auth:
            auth.configure_auth(self._curl)
        if proxy:
            proxy.configure_proxy(self._curl)

    def __del__(self):
        self._curl.close()

    def get(self, url, api, params={}, data=None, request_headers={}):
        http_request = HTTPRequest(method='GET', url=url, api=api, params=params,
                                   data=data, request_headers=request_headers,
                                   additional_curl_options=self._additional_curl_options)
        return http_request(self._curl)

    def post(self, url, api, params={}, data="", request_headers={}):
        http_request = HTTPRequest(method='POST', url=url, api=api, params=params, data=data,
                                   request_headers=request_headers,
                                   additional_curl_options=self._additional_curl_options)
        return http_request(self._curl)

    def put(self, url, api, params={}, data="", request_headers={}):
        http_request = HTTPRequest(method='PUT', url=url, api=api, params=params, data=data,
                                   request_headers=request_headers,
                                   additional_curl_options=self._additional_curl_options)
        return http_request(self._curl)

    def delete(self, url, api, params={}, data=None, request_headers={}):
        http_request = HTTPRequest(method='DELETE', url=url, api=api, params=params, data=data,
                                   request_headers=request_headers,
                                   additional_curl_options=self._additional_curl_options)
        return http_request(self._curl)
