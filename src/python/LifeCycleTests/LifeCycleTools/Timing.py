import time
import json

class TimingStat(object):
    def __init__(self, payload={}, executable=None, query=None):
        self._payload = payload

        if executable and query:
            self._payload.setdefault('stats',self._payload).update({'exe' : str(executable),
                                                                    'query' : str(query)})
    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, type, value, traceback):
        end = time.time()
        self._payload.setdefault('stats',{}).update({'client_request_timing' : end-self.start})

        return False

    def stat_to_file(self, filename):
        with file(filename, 'w') as f:
            json.dump(self._payload, f)

    def stat_to_fileobject(self, fileobject):
        json.dump(self._payload, fileobject)

    def stat_to_server(self, client):
        client.send(self._payload)

    def update_payload(self, value):
        self._payload.setdefault('stats',{}).update(value)

if __name__=="__main__":
    test = {}
    with TimingStat(test):
        time.sleep(1)

    print test
