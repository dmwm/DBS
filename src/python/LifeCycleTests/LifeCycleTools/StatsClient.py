#!/usr/bin/env python
import xmlrpclib

class StatsClient(object):
    def __init__(self, host="localhost", port=9876):
        self.stats_server = xmlrpclib.ServerProxy('http://%s:%s' % (host, port))

    def send(self, stats):
        self.stats_server.add_stats(stats)
        
    def shutdown_server(self):
        self.stats_server.shutdown()

if __name__ == "__main__":
    stats = {'stats':{'query' : "Test"}}
    stats['stats'].update({'api' : 'listPrimaryDSTypes',
                           'client_request_timing' : 1.0,
                           'server_request_timing' : 1.0,
                           'server_request_timestamp' : 123344667,
                           'request_content_length' : 102})
    
    stats_client = StatsClient("localhost", 9876)
    stats_client.send(stats)
    stats_client.shutdown_server()
