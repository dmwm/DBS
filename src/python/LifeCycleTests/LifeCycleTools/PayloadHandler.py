import copy
import json

def increase_interval(start=0.0, step=0.2):
    value = start
    while True:
        yield value
        value += step
        
class PayloadHandler(object):
    def __init__(self):
        self._new_payload = []
        self._initial_payload = None

    def get_payload(self):
        return self._initial_payload

    def load_payload(self, filename):
        with open(filename, 'r') as f:
            self._initial_payload = json.load(f)

    def save_payload(self, filename):
        with open(filename, 'w') as f:
            json.dump(self._new_payload, f)
            
    def clone_payload(self):
        return copy.deepcopy(self._initial_payload)

    def append_payload(self, payload):
        self._new_payload.append(payload)

    payload = property(get_payload)
