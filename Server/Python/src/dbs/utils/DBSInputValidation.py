"""
Input Validation to prohibit SQLInjection, XSS, ...
To use with _validate_input method of the RESTModel implementation
"""
import cjson
import urlparse
#from cherrypy import log
from WMCore.WebTools.Root import WTLogger 
from dbs.utils.dbsExceptionHandler import dbsExceptionHandler
from dbs.utils.dbsException import dbsException,dbsExceptionCode

from WMCore.Lexicon import *
import logging
from WMCore.WebTools.Page import Page

from functools import wraps

def inputChecks(**_params_):
    """
    This is a function to check all the input for GET APIs.
    """
    def checkTypes(_func_, _params_ = _params_):
        log = WTLogger().error_log
        @wraps(_func_)
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
                    #if name =='lumi_list': value = cjson.decode(value)
                    if not isinstance(value, types):
                        serverlog = "Expected '%s' to be %s; was %s." % (name, types, type(value))
                        #raise TypeError, "Expected '%s' to be %s; was %s." % (name, types, type(value))
                        dbsExceptionHandler("dbsException-invalid-input2", message="Invalid Input DataType %s for %s..." %(type(value), name[:5]),\
                                            logger=log.error, serverError=serverlog)
                    else:
                        try:
                            if type(value) == str:
                                if name == 'dataset':
                                    if '*' in value: searchdataset(value)
                                    else: dataset(value)
                                elif name =='lumi_list': value = cjson.decode(value)
                                elif name =='block_name':
                                    if '*' in value: searchblock(value)
                                    else: block(value)
                                elif name =='primary_ds_name':
                                    if '*' in value: searchstr(value)
                                    else: primdataset(value)
                                elif name =='processed_ds_name':
                                    if '*' in value: searchstr(value)
                                    else:  procdataset(value)
                                elif name=='logical_file_name':
                                    if '*' in value: searchstr(value)
                                    else: lfn(value)
                                elif name=='processing_version':
                                    procversion(value)
                                elif name=='global_tag':
                                    if '*' in value: searchstr(value)
                                    else: globalTag(value)
                                elif name == 'create_by':
                                    DBSUser(value)
                                elif name == 'last_modified_by':
                                    DBSUser(value)
                                else:
                                    searchstr(value)
                            elif type(value) == list:
                                if name == 'logical_file_name':
                                    for f in value:
                                        if '*' in f: searchstr(f)
                                        else: lfn(f)
                                elif name == 'block_names':
                                    for block_name in value:
                                        block(block_name)
                        except AssertionError as ae:
                            serverLog = str(ae) + " key-value pair (%s, %s) cannot pass input checking" %(name, value)
                            #print ae
                            dbsExceptionHandler("dbsException-invalid-input2", message="Invalid Input Data %s...: Not Match Required Format" %value[:5],\
                                        serverError=serverLog, logger=log.error)
            return _func_(*args, **kw)
        return wrapped
    return checkTypes

acceptedInputKeys = {
    ################
    'dataTier':['data_tier_name'],
    ################
    'blockBulk':['file_conf_list', 'dataset_conf_list', 'block_parent_list', 'physics_group_name', 'processing_era', 'dataset', 'block', \
                    'acquisition_era', 'primds', 'ds_parent_list', 'files', 'file_parent_list'],
    ################
    'file_conf_list':['release_version', 'pset_hash', 'lfn', 'app_name', 'output_module_label', 'global_tag'],
    ################
    'file_output_config_list':['release_version', 'pset_hash', 'lfn', 'app_name', 'output_module_label', 'global_tag'],
    ################
    'file_parent_list':['file_parent_lfn', 'parent_logical_file_name', 'logical_file_name'],
    ################
    'dataset_conf_list':['release_version', 'pset_hash', 'pset_name', 'app_name', 'output_module_label', 'global_tag'],
    ################
    'output_configs':['release_version', 'pset_hash', 'app_name', 'output_module_label', 'global_tag'],
    ################
    'physics_group_name':[],
    ################
    'processing_era':['processing_version', 'description', 'create_by', 'creation_date'],
    ################
    'dataset':['dataset', 'physics_group_name', 'processed_ds_name', 'dataset_access_type', 'data_tier_name',\
               'output_configs', 'primary_ds_name', 'primary_ds_type', 'acquisition_era_name', 'processing_version', 'xtcrosssection',\
               'create_by', 'creation_date', 'last_modification_date', 'last_modified_by', 'detail', 'prep_id'],
    ################
    'block': ['block_name', 'open_for_writing', 'origin_site_name', 'dataset', 'creation_date', 'creation_date', 'create_by',\
              'last_modification_date', 'last_modified_by', 'file_count', 'block_size'],
    ################
    'acquisition_era':['acquisition_era_name','description', 'start_date','end_date'],
    ################
    'primds':['primary_ds_type', 'primary_ds_name', 'creation_date', 'create_by'],
    ################
    'files':['check_sum', 'file_lumi_list', 'event_count', 'file_type', 'logical_file_name', 'file_size', 'file_output_config_list',\
             'file_parent_list','last_modified_by', 'last_modification_date', 'create_by', 'creation_date', 'auto_cross_section',\
              'adler32', 'dataset', 'block_name', 'md5'],
    ################
    'file_lumi_list':['lumi_section_num', 'run_num'],
    ################
    'migration_rqst':['migration_url','migration_input', 'migration_rqst_id']
    ################
    }

validationFunction = {
    'block_name':block,
    'dataset':dataset,
    'logical_file_name':lfn,
    'file_parent_lfn':lfn,
    'primary_ds_name':primdataset,
    'processed_ds_name':procdataset,
    'processing_version':procversion,
    'acquisition_era_name':acqname,
    'global_tag':globalTag,
    'migration_url':validateUrl
    }

validationFunctionWwildcard = {
    'block_name':searchblock,
    'dataset':searchdataset,
    }


def validateJSONInputNoCopy(input_key,input_data):
    if isinstance(input_data,dict):
        for key in input_data.keys():
            if key not in acceptedInputKeys[input_key]:
                dbsExceptionHandler('dbsException-invalid-input2', message="Invalid Input Key %s..." %key[:5],\
                                    serverError="%s is not a valid input key for %s"%(key, input_key))
            else:
                input_data[key] = validateJSONInputNoCopy(key,input_data[key])
    elif isinstance(input_data,list):
        l = []
        for x in input_data:
            l.append(validateJSONInputNoCopy(input_key,x))
        input_data = l
    elif isinstance(input_data,str):
        validateStringInput(input_key,input_data)
        if '*' in input_data: input_data = input_data.replace('*', '%')
    elif isinstance(input_data,int):
        pass
    elif isinstance(input_data,long):
        pass
    elif isinstance(input_data,float):
        pass
    elif not input_data:
        pass
    else:
        #print  'invalid input: %s= %s'%(input_key, input_data)
        dbsExceptionHandler('dbsException-invalid-input2', message="Invalid Input Data %s..." %input_data[:5], serverError='invalid input: %s= %s'%(input_key, input_data))
    return input_data

def validateStringInput(input_key,input_data):
    """
    To check if a string has the required format. This is only used for POST APIs.
    """
    func = None
    if '*' in input_data or '%' in input_data:
        func = validationFunctionWwildcard.get(input_key)
        if func is None:
            func = searchstr
    elif input_key == 'migration_input' :
        if input_data.find('#') != -1 : func = block
        else : func = dataset
    else:
        func = validationFunction.get(input_key)
        if func is None:
            func = namestr
    try:
        func(input_data)
    except AssertionError as ae:
        serverLog = str(ae) + " key-value pair (%s, %s) cannot pass input checking" %(input_key, input_data)
        #print serverLog
        dbsExceptionHandler("dbsException-invalid-input2", message="Invalid Input Data %s...:  Not Match Required Format" %input_data[:5], serverError=serverLog)
    return input_data



