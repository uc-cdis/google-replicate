import json
import csv

# INDEXD = {
#     "host": "http://localhost:8000",
#     "version": "v0",
#     "auth": {"username": "test", "password": "test"},
# }

INDEXD = {
    "host": "https://nci-crdc.datacommons.io/index/index",
    "version": "v0",
    "auth": {"username": "", "password": ""},
}

GDC_TOKEN = ""

try:
    with open("/home/ubuntu/dcf_dataservice_credentials.json", "r") as f:
        data = json.loads(f.read())
        INDEXD = data.get("INDEXD", {})
        GDC_TOKEN = data.get("GDC_TOKEN", "")
except Exception as e:
    print("Can not read dcf_dataservice_credentials.json file. Detail {}".format(e))

PROJECT_ACL = {}
try:
    with open("./GDC_datasets_access_control.csv", "rt") as f:
        csvReader = csv.DictReader(f, delimiter=",")
        for line in csvReader:
            PROJECT_ACL[line["project_id"]] = line
except Exception as e:
    print("Can not read GDC_datasets_access_control.csv file. Detail {}".format(e))


PROJECT_ACL = {
    "TARGET": {"gs_bucket_prefix": "gdc-target-phs000218"},
    "TCGA": {"gs_bucket_prefix": "gdc-tcga-phs000178"},
    "VAREPOP": {"gs_bucket_prefix": "gdc-varepop-apollo-phs001374"},
    "FM": {"gs_bucket_prefix": "gdc-fm-ad-phs001179"},
    "NCICCR": {"gs_bucket_prefix": "gdc-nciccr-phs001444"},
    "CTSP": {"gs_bucket_prefix": "gdc-ctsp-phs001175"},
    "CCLE": {"gs_bucket_prefix": "gdc-ccle"},
}

