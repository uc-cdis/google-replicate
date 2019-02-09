from socket import error as SocketError
import errno
from multiprocessing import Pool, Manager
from multiprocessing.dummy import Pool as ThreadPool
import time
import timeit
import hashlib
import urllib2
import random
import urllib
import base64
import crcmod
import requests

import threading

import json

from google.cloud import storage
from google.auth.transport.requests import AuthorizedSession

from cdislogging import get_logger
from indexclient.client import IndexClient

from settings import PROJECT_ACL, INDEXD, GDC_TOKEN
import utils
from utils import (
    get_fileinfo_list_from_csv_manifest,
    get_fileinfo_list_from_s3_manifest,
    generate_chunk_data_list,
)
from errors import UserError
from indexd_utils import update_url
from google_resumable_upload import GCSObjectStreamUpload

logger = get_logger("GoogleReplication")

RETRIES_NUM = 10

DATA_ENDPT = "https://api.gdc.cancer.gov/data/"
DEFAULT_CHUNK_SIZE_DOWNLOAD = 1024 * 1024 * 5
DEFAULT_CHUNK_SIZE_UPLOAD = 1024 * 20 * 1024
NUM_TRIES = 10


def prepare_data(manifest_file, global_config):
    """
    Read data file info from manifest and organize them into groups.
    Each group contains files which should be copied to the same bucket
    The groups will be push to the queue consumed by threads
    """
    if manifest_file.startswith("s3://"):
        copying_files = get_fileinfo_list_from_s3_manifest(
            url_manifest=manifest_file,
            start=global_config.get("start"),
            end=global_config.get("end"),
        )
    else:
        copying_files = get_fileinfo_list_from_csv_manifest(
            manifest_file=manifest_file,
            start=global_config.get("start"),
            end=global_config.get("end"),
        )

    if global_config.get("file_shuffle", False):
        random.shuffle(copying_files)

    chunk_size = global_config.get("chunk_size", 1)
    tasks = []

    for idx in range(0, len(copying_files), chunk_size):
        tasks.append(copying_files[idx : idx + chunk_size])

    return tasks, len(copying_files)


class JobInfo(object):
    def __init__(
        self,
        global_config,
        files,
        total_files,
        job_name,
        copied_objects,
        manager_ns,
        bucket=None,
    ):
        """
        Class constructor

        Args:
            global_config(dict): a configuration
            {
                "multi_part_upload_threads": 10,
                "data_chunk_size": 1024*1024*5
            }
            manifest_file(str): manifest file
            thread_num(int): number of threads
            job_name(str): copying|indexing
            bucket(str): source bucket

        """
        self.bucket = bucket
        self.files = files
        self.total_files = total_files
        self.global_config = global_config
        self.job_name = job_name
        self.copied_objects = copied_objects
        self.manager_ns = manager_ns

        self.indexclient = IndexClient(
            INDEXD["host"],
            INDEXD["version"],
            (INDEXD["auth"]["username"], INDEXD["auth"]["password"]),
        )


def get_object_metadata(sess, bucket_name, blob_name):
    """
    get object metadata
    """
    url = "https://www.googleapis.com/storage/v1/b/{}/o/{}".format(
        bucket_name, urllib.quote(blob_name, safe="")
    )
    tries = 0
    while tries < RETRIES_NUM:
        try:
            res = sess.request(method="GET", url=url)
            if res.status_code == 200:
                return res
            else:
                tries += 1
        except Exception:
            tries += 1

    return res


def delete_object(sess, bucket_name, blob_name):
    """
    Delete object from cloud
    """
    url = "https://www.googleapis.com/storage/v1/b/{}/o/{}".format(
        bucket_name, urllib.quote(blob_name, safe="")
    )
    sess.request(method="DELETE", url=url)


def validate_uploaded_data(fi, sess, target_bucket, sig, crc32c, sorted_results):
    """
    validate uploaded data

    Args:
        fi(dict): file info
        sess(session): google client session
        target_bucket(str): aws bucket
        sig(sig): md5 of downloaded data from api
        sorted_results(list(object)): list of result returned by upload function

    Returns:
       bool: pass or not
    """

    md5_digests = []
    total_bytes_received = 0

    for res, part_number, md5, chunk_size in sorted_results:
        md5_digests.append(md5)
        total_bytes_received += chunk_size

    # compute local etag from list of md5s
    #etags = hashlib.md5(b"".join(md5_digests)).hexdigest() + "-" + str(len(md5_digests))
    object_path = "{}/{}".format(fi["id"], fi["file_name"])

    sig_check_pass = True

    if sig_check_pass:
        meta_data = get_object_metadata(sess, target_bucket, object_path)
        if meta_data.status_code != 200:
            return False

        if int(meta_data.json().get("size", "0")) != fi["size"]:
            logger.warn(
                "Can not stream the object {}. {} vs {}. Size does not match".format(fi.get("id"), int(meta_data.json().get("size", "0")), fi["size"])
            )
            sig_check_pass = False

    if sig_check_pass:
        if meta_data.json().get("crc32c", "") != base64.b64encode(crc32c.digest()):
            logger.warn(
                "Can not stream the object {} to {}. crc32c check fails".format(
                    fi.get("id"), target_bucket
                )
            )
            sig_check_pass = False

    if sig_check_pass:
        if sig.hexdigest() != fi.get("md5"):
            logger.warn(
                "Can not stream the object {}. md5 check fails".format(fi.get("id"))
            )
            sig_check_pass = False

    return sig_check_pass


def check_and_index_the_data(jobinfo):
    """
    Check if files are in manifest are copied or not.
    Index the files if they exists in target buckets and log
    """
    json_log = {}
    for fi in jobinfo.files:
        object_path = "{}/{}".format(fi.get("id"), fi.get("file_name"))
        if object_path not in jobinfo.copied_objects:
            json_log[object_path] = {"copy_success": False, "index_success": False}
        else:
            try:
                json_log[object_path] = {
                    "copy_success": True,
                    "index_success": update_url(fi, jobinfo.indexclient),
                }
            except Exception as e:
                logger.error(e.message)
                json_log[object_path] = {
                    "copy_success": True,
                    "index_success": False,
                    "msg": e.message,
                }

    jobinfo.manager_ns.total_processed_files += len(jobinfo.files)
    logger.info(
        "{}/{} object are processed/indexed ".format(
            jobinfo.manager_ns.total_processed_files, jobinfo.total_files
        )
    )

    return json_log


def check_blob_name_exists_and_match_size(sess, bucket_name, blob_name, fi):
    """
    check if blob object exists or not
    Args:
        bucket_name(str): the name of bucket
        blob_name(str): the name of blob
        fi(dict): file info dictionary
    Returns:
        bool: indicating value if the blob is exist or not
    """
    url = "https://www.googleapis.com/storage/v1/b/{}/o/{}".format(
        bucket_name, urllib.quote(blob_name, safe="")
    )
    res = sess.request(method="GET", url=url)
    return (
        res.status_code == 200
        and int(res.json()["size"]) == fi.get("size")
    )


def fail_resumable_copy_blob(sess, bucket_name, blob_name, fi):
    """
    Something wrong during the copy(only for simple upload and resumable upload)
    Args:
        bucket_name(str): the name of bucket
        blob_name(str): the name of blob
        fi(dict): file info dictionary
    Returns:
        bool: indicating value if the blob is exist or not
    """
    url = "https://www.googleapis.com/storage/v1/b/{}/o/{}".format(
        bucket_name, urllib.quote(blob_name, safe="")
    )
    res = sess.request(method="GET", url=url)
    return (
        res.status_code == 200
        and base64.b64decode(res.json()["md5Hash"]).encode("hex") == fi.get("md5")
    )


def exec_google_copy(jobinfo):
    """
    Stream a list of files from the gdcapi to the google buckets.
    The target buckets are infered from PROJECT_ACL and project_id in the file

    Intergrity check:
        - Streaming:
            +) Compute local crc32c and match with the one provided by google
            +) Compute md5 on the fly to check the intergrity of streaming data
                from gdcapi to local machine

    Args:
        jobinfo(JobInfo): Job info

    Returns:
        None
    """
    client = storage.Client()
    sess = AuthorizedSession(client._credentials)

    files = jobinfo.files
    for fi in files:
        try:
            target_bucket = utils.get_google_bucket_name(fi, PROJECT_ACL)
        except UserError as e:
            logger.warn(e)
            continue

        blob_name = "{}/{}".format(fi["id"], fi["file_name"])
        if check_blob_name_exists_and_match_size(sess, target_bucket, blob_name, fi):
            logger.info(
                "{} is already copied ".format(fi["id"])
            )
            continue

        logger.info("Start streaming the object {}. Size {} (GB)".format(fi["id"], fi["size"]*1.0/1000/1000/1000))

        try:
            begin = timeit.default_timer()

            resumable_streaming_copy(fi, client, target_bucket, blob_name, jobinfo.global_config)
            if fail_resumable_copy_blob(sess, target_bucket, blob_name, fi):
                delete_object(sess, target_bucket, blob_name)
            else:
                end = timeit.default_timer()
                logger.info("Finish streaming the object {}. Size {} (GB)".format(fi["id"], fi["size"]*1.0/1000/1000/1000))
                logger.info("Throughput {}(MB)/s".format(fi["size"]/(end-begin)/1000/1000))
    
        except Exception as e:
            #Don't break
            logger.warn("Can not stream {}. Detail {}".format(fi["id"], e))

    jobinfo.manager_ns.total_processed_files += len(files)
    logger.info(
        "{}/{} object are processed/copying ".format(
            jobinfo.manager_ns.total_processed_files, jobinfo.total_files
        )
    )

    return len(files)

def resumable_streaming_copy(fi, client, bucket_name, blob_name, global_config):
    """
    Copy file to google bucket. Implemented using google cloud resumale API
    Args:
        fi(dict): file information
        global_config(dict): configurations
            {
                "chunk_size_download": 1024,
                "chunk_size_upload": 1024
            }
    Returns: None
    """

    chunk_size_download = global_config.get(
        "chunk_size_download", DEFAULT_CHUNK_SIZE_DOWNLOAD
    )
    chunk_size_upload = global_config.get(
        "chunk_size_upload", DEFAULT_CHUNK_SIZE_UPLOAD
    )
    data_endpt = DATA_ENDPT + fi.get("id", "")
    token = GDC_TOKEN

    tries = 0
    while tries < NUM_TRIES:
        try:
            response = requests.get(
                data_endpt,
                stream=True,
                headers={"Content-Type": "application/json", "X-Auth-Token": token},
            )
            # Too many requests
            if response.status_code == 429:
                time.sleep(60)
                tries += 1
            else:
                break
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            time.sleep(5)
            tries += 1
        except Exception as e:
            raise Exception("Can not setup connection to gdcapi for {}. Detail {}".format(fi["id"]), e)

    if tries == NUM_TRIES:
        raise Exception("Can not setup connection to gdcapi for {}".format(fi["id"]))

    if response.status_code != 200:
        raise Exception("GDCPotal error {}".format(response.status_code))

    try:
        streaming(
            client,
            response,
            bucket_name,
            chunk_size_download,
            chunk_size_upload,
            blob_name,
            fi["size"],
        )
    except Exception:
        raise Exception(
            "GCSObjectStreamUpload: Can not upload {}".format(fi.get("id", ""))
        )


def streaming(
    client, response, bucket_name, chunk_size_download, chunk_size_upload, blob_name, total_size
):
    """
    stream the file with google resumable upload
    Args:
        client(GSClient): gs storage client
        response(httpResponse): http response
        bucket_name(str): target google bucket name
        chunk_size_download(int): chunk size in bytes from downling data file
        blob_name(str): object name
    Returns:
        None
    """

    # keep track the number of chunks uploaded
    with GCSObjectStreamUpload(
        client=client,
        bucket_name=bucket_name,
        blob_name=blob_name,
        chunk_size=chunk_size_upload,
    ) as s:
        progress = 0
        number_upload = 0
        for chunk in response.iter_content(chunk_size=chunk_size_download):
            if chunk:  # filter out keep-alive new chunks
                progress += s.write(chunk)
                number_upload += 1
                if number_upload % 1000 == 0:
                    logger.info("Uploading {}. Progress {}".format(blob_name, 100.0*progress/total_size))


def _is_completed_task(sess, task):
    
    for fi in task:
        try:
            target_bucket = utils.get_google_bucket_name(fi, PROJECT_ACL)
        except UserError as e:
            logger.warn(e)
            continue

        blob_name = "{}/{}".format(fi["id"], fi["file_name"])
        res = get_object_metadata(sess, target_bucket, blob_name)
        if res.status_code != 200:
            return False
    
    return True


def run(thread_num, global_config, job_name, manifest_file, bucket=None):
    """
    start threads and log after they finish
    """

    tasks, _ = prepare_data(manifest_file, global_config)

    manager = Manager()
    manager_ns = manager.Namespace()
    manager_ns.total_processed_files = 0

    client = storage.Client()
    sess = AuthorizedSession(client._credentials)

    jobInfos = []
    for task in tasks:
        if global_config.get("scan_copied_objects", False):
            if _is_completed_task(sess, task):
                continue
        job = JobInfo(
            global_config, task, len(tasks), job_name, {}, manager_ns, bucket
        )
        jobInfos.append(job)

    # Make the Pool of workers
    pool = Pool(thread_num)

    results = []

    if job_name == "copying":
        results = pool.map(exec_google_copy, jobInfos)
    elif job_name == "indexing":
        results = pool.map(check_and_index_the_data, jobInfos)

    # close the pool and wait for the work to finish
    pool.close()
    pool.join()

    filename = global_config.get("log_file", "{}_log.json".format(job_name))

    timestr = time.strftime("%Y%m%d-%H%M%S")
    filename = timestr + "_" + filename

    if job_name == "copying":
        results = [{"data": results}]

    json_log = {}

    for result in results:
        json_log.update(result)
