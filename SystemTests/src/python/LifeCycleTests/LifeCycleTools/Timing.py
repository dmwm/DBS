from __future__ import print_function
import time
import json


class TimingStat(object):
    def __init__(self, stats=None, client=None, stats_name="stats"):
        self._stats = stats
        self._client = client
        self._stats_name = stats_name

    def __enter__(self):
        self.start = time.time()
        self._stats.setdefault(self._stats_name, {}).update({'start_time': self.start})
        return self

    def __exit__(self, tb_type, tb_value, tb):
        end = time.time()
        self._stats.setdefault(self._stats_name, {}).update({'client_request_timing': end-self.start})
        self._stats.setdefault(self._stats_name, {}).update({'end_time': end})

        if tb_type and self._client:
            #replace "\'" in dbs client exception, since it leads to a crash, while injection in sqlite
            #needs to be fixed in client code
            tb_value = str(tb_value).replace("\'", " ")
            self._stats.setdefault('failures', {}).update({'type': str(tb_type),
                                                           'value': str(tb_value),
                                                           'traceback': str(tb)})
            self.stat_to_server()

        return False

    def stat_to_file(self, filename):
        with file(filename, 'w') as f:
            json.dump(self._stats, f)

    def stat_to_fileobject(self, fileobject):
        json.dump(self._stats, fileobject)

    def stat_to_server(self):
        self._client.send(self._stats)

    def update_stats(self, value):
        self._stats.setdefault(self._stats_name, {}).update(value)

if __name__ == "__main__":
    test = {}
    with TimingStat(test):
        time.sleep(1)

    print(test)
