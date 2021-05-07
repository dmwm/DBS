#!/usr/bin/env python
from optparse import OptionParser
import urllib.request, urllib.parse, urllib.error
import urllib.parse
import yaml
import json, os, re, sys
import pprint

def get_command_line_options(executable_name, arguments):
    parser = OptionParser(usage="%s options" % executable_name)
    parser.add_option("-i", "--in", type="string", dest="input", help="Input JSON")
    parser.add_option("-o", "--out", type="string", dest="output", help="Output JSON")
    parser.add_option("-m", "--mapping", type="string", dest="mapping", help="DAS Mapping File")

    (options, args) = parser.parse_args()

    error_msg = """You need to provide following options, --in=input.json (mandatory), --mapping (mandatory), --out=output.json (optional)\n"""

    if not (options.input and options.mapping):
        parser.print_help()
        parser.error(error_msg)

    return options

class DASMapping(object):
    def __init__(self, mapfile):
        self._mapfile = mapfile
        self._das_map = {}
        self._create_das_mapping()

    def _create_das_mapping(self):
        """
        das_map = {'lookup' : [{params : {'param1' : 'required', 'param2' : 'optional', 'param3' : 'default_value' ...},
                                url : 'https://cmsweb.cern.ch:8443/dbs/prod/global/DBSReader/acquisitioneras/',
                                das_map : {'das_param1' : dbs_param1, ...}
                                }]
                                }
        """
        with open(self._mapfile, 'r') as f:
            for entry in yaml.load_all(f):
                das2dbs_param_map = {}
                if 'lookup' not in entry:
                    continue
                for param_map in entry['das_map']:
                    if 'api_arg' in param_map:
                        das2dbs_param_map[param_map['das_key']] = param_map['api_arg']

                self._das_map.setdefault(entry['lookup'], []).append({'params' : entry['params'],
                                                                     'url' : entry['url'],
                                                                     'das2dbs_param_map' : das2dbs_param_map})

    def create_dbs_query(self, lookup, das_params):
        apis = self._das_map[lookup]
        matching_api = None

        #run parameter is yet not finalized in DBS3/DAS
        #translate to minrun, maxrun
        try:
            run = das_params['run']
        except KeyError:
            pass
        else:
            das_params['minrun'] = run
            das_params['maxrun'] = run
            del das_params['run']

        for api_call in apis:
            ###DAS and DBS3 do not use the parameters, for example block in DAS vs. block_name in DBS3
            ###Needs translation using das3dbs_param_map
            das2dbs_param_map = api_call['das2dbs_param_map']
            das2dbs_key_changer = lambda key, map=das2dbs_param_map: map[key] if key in map else key
            das_param_keys = set(map(das2dbs_key_changer, list(das_params.keys())))

            api_params = set(api_call['params'].keys())

            if das_param_keys.issubset(api_params):
                matching_api = api_call
                break

        if not matching_api:
            return None, None

        dbs_params = {}

        for param in list(matching_api['params'].keys()):
            try:
                dbs_params[param] = das_params[param]
            except KeyError as ke:
                if matching_api['params'][param] == 'required':
                    raise ke
                if matching_api['params'][param] != 'optional':
                    dbs_params[param] = matching_api['params'][param]

        dbs_url = urllib.parse.urlparse(matching_api['url'])
        path = dbs_url.path
        api_pattern = re.compile(r'^/dbs/\S+/\S+/DBSReader/(?P<api>\S+)/.*')
        match_obj = api_pattern.match(path)
        path = match_obj.groupdict()
        dbs_api = path['api']

        return dbs_api, dbs_params

class DBSMapping(object):
    def __init__(self):
        self._dbs_map = {'blocks' : 'listBlocks',
                         'datasets' : 'listDatasets',
                         'files' : 'listFiles',
                         'releaseversions' : 'listReleaseVersions',
                         'runs' : 'listRuns'
                         }

    def map_api_call(self, api_call):
        return self._dbs_map[api_call]

def extract_das_parameters(das_query):
    das_params = {}
    lookup = None
    parameters = das_query.split('|')[0] #ignore grep or sort parameters like grep run.run_number
    whitespace_pattern = re.compile(r'(\s+=\s+|\s+=|=\s+)')
    parameters = re.sub(whitespace_pattern, '=', parameters) #convert dataset = /bla to dataset=/bla
    parameters = parameters.strip().split()

    for parameter in parameters:
        try:
            key, value = parameter.strip().split('=', 1)
        except ValueError:
            lookup = parameter
        else:
            if key not in ('instance',):
                das_params.update({key : value})

    if not lookup:
        for keyword in ('dataset', 'block', 'file'):
            try:
                das_params[keyword]
            except KeyError:
                pass
            else:
                lookup=keyword
                break

    return lookup, das_params

if __name__ == '__main__':
    request_parts = [r'(?P<method>\S+)',    #http method used
                     r'(?P<uri>\S+)',       #request uri
                     r'(?P<version>.*)',    #protocol version used
                     ]

    request_pattern = re.compile(r'\s+'.join(request_parts)+r'\s*\Z')

    options = get_command_line_options(os.path.basename(__file__), sys.argv)

    das_mapping = DASMapping(options.mapping)
    dbs_mapping = DBSMapping()

    dbs_calls = []

    with open(options.input, 'r') as f:
        log_entries = json.load(f)

        for log_entry in log_entries:
            request = log_entry['request']
            match_obj = request_pattern.match(request)
            request = match_obj.groupdict()
            uri = request['uri']
            parsed_uri = urllib.parse.urlparse(uri)
            parsed_query_string = urllib.parse.parse_qs(parsed_uri.query)
            lookup, das_params = extract_das_parameters(parsed_query_string['input'][0])
            dbs_api, dbs_params = das_mapping.create_dbs_query(lookup, das_params)
            if dbs_api:
                dbs_calls.append({dbs_mapping.map_api_call(dbs_api) : dbs_params})

        if not options.output:
            pprint.pprint(dbs_calls)
        else:
            with open(options.output, 'w') as f:
                json.dump(dbs_calls, f)
