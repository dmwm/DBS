"""
Regular expression for DBS input validation
"""
import re

###validation of strings
string_validation_rx = re.compile(r"^[-A-Za-z0-9_]+$")

###validation of datasets
dataset_validation_rx = re.compile(r"^/(\*|[a-zA-Z\*][a-zA-Z0-9_\*\-]{0,100})(/(\*|[a-zA-Z0-9_\.\-\*]{1,100})){0,1}(/(\*|[A-Z\-\*]{1,50})){0,1}$")

###validation of logical file names
lfnParts = {
    'era'           : r'([a-zA-Z0-9\-_]+)',
    'primDS'        : r'([a-zA-Z0-9\-_]+)',
    'tier'          : r'([A-Z\-_]+)',
    'version'       : r'([a-zA-Z0-9\-_]+)',
    'secondary'     : r'([a-zA-Z0-9\-_]+)',
    'counter'       : r'([0-9]+)',
    'root'          : r'([a-zA-Z0-9\-_]+).root',
    'hnName'        : r'([a-zA-Z0-9\.]+)',
    'subdir'        : r'([a-zA-Z0-9\-_]+)',
    'file'          : r'([a-zA-Z0-9\-\._]+)',
    'workflow'      : r'([a-zA-Z0-9\-_]+)',
    'physics_group' : r'([a-zA-Z\-_]+)',
}

lfn_rx1 = r'^/([a-z]+)/([a-z0-9]+)/([a-zA-Z0-9\-_]+)/([a-zA-Z0-9\-_]+)/([A-Z\-_]+)/([a-zA-Z0-9\-_]+)((/[0-9]+){3}){0,1}/([0-9]+)/([a-zA-Z0-9\-_]+).root$'
lfn_rx2 = r'^/([a-z]+)/([a-z0-9]+)/([a-z0-9]+)/([a-zA-Z0-9\-_]+)/([a-zA-Z0-9\-_]+)/([A-Z\-_]+)/([a-zA-Z0-9\-_]+)((/[0-9]+){3}){0,1}/([0-9]+)/([a-zA-Z0-9\-_]+).root$'
lfn_rx3 = r'^/store/(temp/)*(user|group)/%(hnName)s/%(primDS)s/%(secondary)s/%(version)s/%(counter)s/%(root)s$' % lfnParts

oldStyleTier0LFN = r'^/store/data/%(era)s/%(primDS)s/%(tier)s/%(version)s/%(counter)s/%(counter)s/%(counter)s/%(root)s$' % lfnParts
tier0LFN = r'^/store/(backfill/[0-9]/){0,1}(t0temp/){0,1}(data|express|hidata)/%(era)s/%(primDS)s/%(tier)s/%(version)s/%(counter)s/%(counter)s/%(counter)s/%(counter)s/%(root)s$' % lfnParts

storeResultsLFN = r'^/store/results/%(physics_group)s/%(primDS)s/%(secondary)s/%(primDS)s/%(tier)s/%(secondary)s/%(counter)s/%(root)s$' % lfnParts
filename_validation_rx = re.compile(r"(%s|%s|%s|%s|%s|%s)" % (lfn_rx1, lfn_rx2, lfn_rx3, oldStyleTier0LFN, tier0LFN, storeResultsLFN))

### validation of run
run_validation_rx = re.compile(r"^[0-9\-]+$")

### processing version
processing_version_validation_rx = re.compile(r"^[0-9]+$")

### acquisition_era
acquisition_era_validation_rx = re.compile(r"[a-zA-Z][a-zA-Z0-9_]*$")

### primary_ds_name
primary_ds_name_validation_rx = re.compile(r"^[a-zA-Z][a-zA-Z0-9\-_]*$")

### processed_ds_name
processed_ds_name_validation_rx = re.compile(r"[a-zA-Z][a-zA-Z0-9_]*(\-[a-zA-Z0-9_]+){0,2}-v[0-9]*$")

### data tier name
data_tier_name_validation_rx = re.compile(r"[A-Z\-_]+")

### release version
release_version_validation_rx = re.compile("CMSSW(_\d+){3}(_[a-zA-Z0-9_]+)?$")
