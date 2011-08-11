"""
Input Validation to prohibit SQLInjection, XSS, ...
To use with _validate_input method of the RESTModel implementation
"""
import cjson
from cherrypy import log
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.dbsException import dbsException,dbsExceptionCode

from WMCore.Lexicon import *

def inputChecks(**_params_):
    def checkTypes(_func_, _params_ = _params_):
        def wrapped(*args, **kw):
            arg_names = _func_.func_code.co_varnames[:_func_.func_code.co_argcount]
            ka = {}
            ka.update(zip(arg_names, args))
            ka.update(kw)
            #print ka
            for name, value in ka.iteritems():
                #In fact the framework removes all the input variables that is not in the args list of _addMethod.
                #So DBS list API will never see these variables. For example, if one has
                #http://hostname/cms_dbs/DBS/datatiers?name=abc, the API will get a request to list all the datatiers because
                #"name=abc" is removed by the framework since name is not a key work for the api.
                if name !='self':
                    types = _params_[name]
                    if name =='lumi_list': value = cjson.decode(value)
                    if not isinstance(value, types):
                        serverlog = "Expected '%s' to be %s; was %s." % (name, types, type(value))
                        #raise TypeError, "Expected '%s' to be %s; was %s." % (name, types, type(value))
                        dbsExceptionHandler("dbsException-invalid-input", "Invalid Input DataType", log, serverlog)
                    else:
                        if type(value) == str:
                            try:
                                searchingstr(value)
                            except AssertionError as ae:
                                serverLog = str(ae) + " key-value pair (%s, %s) cannot pass input checking" %(input_key, input_data)
                                print ae
                                dbsExceptionHandler("dbsException-invalid-input", "Invalid Input Data: Not Match Required Format",\
                                    log, serverLog)
            return _func_(*args, **kw)
        return wrapped
    return checkTypes

@inputChecks(data_tier_name=str, block_name=str, run_num=int)
def inputValidation(data_tier_name=""):
    return inputArray

validArgs = {
    ################
    'listDataTiers':['data_tier_name']
    ################
    }

requiredInputKeys = {
    ################
    'insertDataTier':['data_tier_name'],
    ################
    'insertBlock':['file_conf_list', 'dataset_conf_list', 'block_parent_list', 'physics_group_name', 'processing_era', 'dataset', 'block', 'acquisition_era', 'primds', 'ds_parent_list', 'files', 'file_parent_list'],
    ################
    'file_conf_list':['release_version', 'pset_hash', 'lfn', 'app_name', 'output_module_label', 'global_tag'],
    ################
    'dataset_conf_list':['release_version', 'pset_hash', 'app_name', 'output_module_label', 'global_tag'],
    ################
    'physics_group_name':[],
    ################
    'processing_era':['processing_version'],
    ################
    'dataset':['dataset', 'is_dataset_valid', 'physics_group_name', 'processed_ds_name', 'dataset_access_type', 'data_tier_name'],
    ################
    'block': ['block_name', 'open_for_writing', 'origin_site_name'],
    ################
    'acquisition_era':['acquisition_era_name'],
    ################
    'primds':['primary_ds_type', 'primary_ds_name'],
    ################
    'files':['check_sum', 'file_lumi_list', 'event_count', 'file_type', 'logical_file_name', 'file_size'],
    ################
    'file_lumi_list':['lumi_section_num', 'run_num']
    ################
    }

validationFunction = {
    'block_name':block,
    'dataset':dataset
    #'logical_file_name':lfn
    }

def validateJSONInput(input_key,input_data):
    if isinstance(input_data,dict):
        return_data = {}
        for key in requiredInputKeys[input_key]:
            try:
                return_data[key] = validateJSONInput(key,input_data[key])
            except KeyError as ke:
                dbsExceptionHandler('dbsException-invalid-input', "Invalid input")

    elif isinstance(input_data,list):
        return_data = [validateJSONInput(input_key,x) for x in input_data]

    elif isinstance(input_data,str):
        return_data = validateStringInput("", input_data)

    elif isinstance(input_data,int):
        return input_data
    
    else:
        dbsExceptionHandler('dbsException-invalid-input', "Invalid input")
            
    return return_data

def validateJSONInputNoCopy(input_key,input_data):
    if isinstance(input_data,dict):
        for key in input_data.keys():
            if key not in requiredInputKeys[input_key]:
                dbsExceptionHandler('dbsException-invalid-input', "Invalid input")
            else:
                validateJSONInputNoCopy(key,input_data[key])
                
    elif isinstance(input_data,list):
        for x in input_data:
            validateJSONInputNoCopy(input_key,x) 

    elif isinstance(input_data,str):
        validateStringInput(input_key,input_data)

    elif isinstance(input_data,int):
        pass
    
    else:
        dbsExceptionHandler('dbsException-invalid-input', "Invalid input")

    return

def validateStringInput(input_key,input_data):
    """
    To check if a string has the required format. For searching purpose, we will allow wildcards.
    """
    func = validationFunction.get(input_key)
    if func is None:
        func = searchingstr

    try:
        func(input_data)
    except AssertionError as ae:
        serverLog = str(ae) + " key-value pair (%s, %s) cannot pass input checking" %(input_key, input_data)
        #raise ValueError, serverLog
        print ae
        dbsExceptionHandler("dbsException-invalid-input", "Invalid Input Data: Not Match Required Format", log, serverLog)
    return input_data

    
        
    
