import time
import json

class TimingStat(object):
    def __init__(self, stats={}, client=None):
        self._stats = stats
        self._client = client

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, tb_type, tb_value, tb):
        end = time.time()
        self._stats.setdefault('stats',{}).update({'client_request_timing' : end-self.start})

        if tb_type and self._client:
            failures = {'failures': {'type':str(tb_type),
                                     'value':str(tb_value),
                                     'traceback':str(tb)}}
            self._client.send(failures)

        return False

    def stat_to_file(self, filename):
        with file(filename, 'w') as f:
            json.dump(self._stats, f)

    def stat_to_fileobject(self, fileobject):
        json.dump(self._stats, fileobject)

    def stat_to_server(self):
        self._client.send(self._stats)

    def update_stats(self, value):
        self._stats.setdefault('stats',{}).update(value)

if __name__=="__main__":
    test = {}
    with TimingStat(test):
        time.sleep(1)

    print test
