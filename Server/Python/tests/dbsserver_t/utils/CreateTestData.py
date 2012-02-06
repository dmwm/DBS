import os
import cPickle
import uuid

persistent_data = {'physics_group':
             [{'physics_group_name': 'Bphys'}, {'physics_group_name': 'Btag'}, {'physics_group_name': 'Diffraction'}, {'physics_group_name': 'EWK'}, {'physics_group_name': 'Egamma'}, {'physics_group_name': 'HeavyIon'}, {'physics_group_name': 'Higgs'}, {'physics_group_name': 'Individual'}, {'physics_group_name': 'JetMet'}, {'physics_group_name': 'Muons'}, {'physics_group_name': 'OnSel'}, {'physics_group_name': 'PFlowTau'}, {'physics_group_name': 'PhysVal'}, {'physics_group_name': 'QCD'}, {'physics_group_name': 'RelVal'}, {'physics_group_name': 'SUSYBSM'}, {'physics_group_name': 'Top'}, {'physics_group_name': 'Tracker'}],
             'primary_ds_type':
             [{u'data_type': 'MC'}, {u'data_type': 'DATA'}, {u'data_type': 'TEST'}, {u'data_type': 'mc'}, {u'data_type': 'data'}, {u'data_type': 'test'}]
            }

acquisition_era_name = "DBS3UNITTESTACQERA_@unique_id@"
processing_version = "@unique_id_9999@"
primary_ds_name = "DBS3UnitTestPrimary_@unique_id@"
processed_ds_name = "%s-DBS3UnitTestProcessedDataset_@unique_id@-v%s" %(acquisition_era_name,processing_version)
child_processed_ds_name = "%s-DBS3UnitTestProcessedChildDataset_@unique_id@-v%s" % (acquisition_era_name,processing_version)
data_tier_name = "DBS3-UNIT-TEST-TIER-@unique_id@"
dataset_name = "/%s/%s/%s" % (primary_ds_name, processed_ds_name, data_tier_name)
child_dataset_name = "/%s/%s/%s" % (primary_ds_name, child_processed_ds_name, data_tier_name)
block_name = "/%s/%s/%s#%s" % (primary_ds_name, processed_ds_name, data_tier_name, "@unique_hash@")
child_block_name = "/%s/%s/%s#%s" % (primary_ds_name, child_processed_ds_name, data_tier_name, "@unique_hash@")
logical_file_name = "/store/temp/%s/%s/%s/DBS3UnitTest/file_@date@.root" % (primary_ds_name, processed_ds_name, data_tier_name)
child_logical_file_name = "/store/temp/%s/%s/%s/DBS3UnitTest/file_@date@.root" % (primary_ds_name, child_processed_ds_name, data_tier_name)

transient_data = {'acquisition_era':
                 [{
                      'acquisition_era_name' : acquisition_era_name,
                      'creation_date' : "@date@",
                      'create_by' : '@user@',
                      'description' : 'DBS3UnitTestAcqEra_@unique_id@',
                      'start_date' : '@date@',
                      'end_date' : None
                 }],
                  'block' :
                 [{
                      'block_name' : block_name,
                      'dataset' : dataset_name,
                      'last_modification_date' : "@date@",
                      'last_modified_by' : "@user@",
                      'create_by' : "@user@",
                      'creation_date' : "@date@",
                      'open_for_writing' : 1,
                      'block_size' : 0,
                                    'file_count' : 0,
                      'origin_site_name' : 'DBS3UnitTestSite'                 
                 }],
                  'block_parentage' :
                 [{
                      'block_name' : child_block_name,
                      'parent_block_name' : block_name
                 }],
                  'child_block' :
                 [{
                      'block_name' : child_block_name,
                      'dataset' : child_dataset_name,
                      'last_modification_date' : "@date@",
                      'last_modified_by' : "@user@",
                      'create_by' : "@user@",
                      'creation_date' : "@date@",
                      'open_for_writing' : 1,
                      'block_size' : 0,
                      'file_count' : 0,
                      'origin_site_name' : 'DBS3UnitTestSite'                 
                 }],
                  'child_dataset' :
                 [{
                      "primary_ds_name" : primary_ds_name,
                      "primary_ds_type" : "mc",
                      "processed_ds_name" : child_processed_ds_name,
                      "data_tier_name" : data_tier_name,
                      "dataset" : child_dataset_name,
                      "dataset_access_type" : "VALID",
                      "xtcrosssection" : None,
                      "prep_id" : None,
                      "physics_group_name" : None,
                      "acquisition_era_name" : acquisition_era_name,
                      "processing_version": processing_version,
                      "creation_date" : "@date@",
                      "create_by" : "@user@",
                      "last_modification_date" : "@date@",
                      "last_modified_by": "@user@"
                 }],
                  'child_file' :
                 [{
                      "logical_file_name" : child_logical_file_name,
                      "is_file_valid" : 1,
                      "check_sum" : "@unique_id@", 
                      "event_count" : "@unique_id@", 
                      "file_size" : "@unique_id@",
                      "file_type" : "EDM",
                      "adler32" : "@unique_id@", 
                      "md5" : "@unique_id@",
                      "auto_cross_section" : "@unique_id@",
                      "creation_date" : None, #See Ticket #965 YG.
                      "create_by": None, #See Ticket #965 YG.
                      "last_modification_date": "@date@", 
                      "last_modified_by" : "@user@",
                      "dataset" : child_dataset_name,
                      "block_name" : child_block_name
                 }],
                  'dataset':
                 [{
                      "primary_ds_name" : primary_ds_name,
                      "primary_ds_type" : "mc",
                      "processed_ds_name" : processed_ds_name,
                      "data_tier_name" : data_tier_name,
                      "dataset" : dataset_name,
                      "dataset_access_type" : "VALID",
                      "xtcrosssection" : None,
                      "prep_id" : None,
                      "physics_group_name" : None,
                      "acquisition_era_name" : acquisition_era_name,
                      "processing_version": processing_version,
                      "creation_date" : "@date@",
                      "create_by" : "@user@",
                      "last_modification_date" : "@date@",
                      "last_modified_by": "@user@"
                 }],
                  'dataset_parentage' :
                 [{
                      'dataset' : child_dataset_name,
                      'parent_dataset' : dataset_name
                 }],
                  'data_tier':
                 [{
                      "data_tier_name" : data_tier_name,
                      'creation_date' : "@date@",
                      'create_by' : '@user@'
                 }],
                  'file':
                 [{
                      "logical_file_name" : logical_file_name,
                      "is_file_valid" : 1,
                      "check_sum" : "@unique_id@", 
                      "event_count" : "@unique_id@", 
                      "file_size" : "@unique_id@",
                      "file_type" : "EDM",
                      "adler32" : "@unique_id@", 
                      "md5" : "@unique_id@",
                      "auto_cross_section" : "@unique_id@",
                      "creation_date" : None, #See Ticket #965 YG.
                      "create_by": None, #See Ticket #965 YG.
                      "last_modification_date": "@date@", 
                      "last_modified_by" : "@user@",
                      "dataset" : dataset_name,
                      "block_name" : block_name
                 }],
                  'file_lumi':
                 [{
                      "lumi_section_num": "@unique_id_9999@", 
                      "run_num": "@unique_id_9999@"
                 }], 
                  'file_parentage':
                 [{
                      "logical_file_name" : child_logical_file_name,
                      "parent_logical_file_name" : logical_file_name
                 }],
                  'output_module_config':
                 [{
                      "release_version" : "CMSSW_1_2_@unique_id@",
                      "pset_hash" : "@unique_hash@",
                      "app_name" : "cmsRun",
                      "output_module_label" : "DBS3UnitTestLabel_@unique_id@",
                      "global_tag" : "DBS3UnitTestTag_@unique_id@",
                      "creation_date" : "@date@",
                      "create_by" : "@user@"
                 }],
                  'primary_dataset':
                 [{
                      "primary_ds_type" : "mc",
                      "primary_ds_name" : primary_ds_name,
                      "creation_date" : "@date@",
                      "create_by" : "@user@"
                 }],
                  'processing_era':
                 [{
                      "processing_version" : processing_version,
                      "creation_date" : '@date@',
                      "create_by" : '@user@',
                      "description" : "DBS3UnitTestProcEra_@unique_id_9999@"
                 }]
             }

f = file(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../data/template_transient_test_data.pkl'), "w")
cPickle.dump(transient_data, f)
f.close()

f = file(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../data/persistent_test_data.pkl'), "w")
cPickle.dump(persistent_data, f)
f.close()

