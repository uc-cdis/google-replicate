from socket import error as SocketError
import errno
from multiprocessing import Pool, Manager
from multiprocessing.dummy import Pool as ThreadPool
import time
import timeit
import hashlib
import urllib2
import queue
import urllib
import base64
import crcmod

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

logger = get_logger("GoogleReplication")

RETRIES_NUM = 5


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


def upload_chunk_to_gs(sess, chunk_data, bucket, key, part_number):
    """
    upload to create compose object.

    Args:
        sess(session): google client session
        chunk_data: chunk data
        bucket(str): bucket name
        key(str): key
        part_number(int): part number

    Return:
        http.Response
    """
    object_part_name = key + "-" + str(part_number)
    url = "https://www.googleapis.com/upload/storage/v1/b/{}/o?uploadType=media&name={}".format(
        bucket, object_part_name
    )
    headers = {
        "Content-Type": "application/octet-stream",
        "Content-Length": str(len(chunk_data)),
    }
    tries = 0
    while tries < RETRIES_NUM:
        res = sess.request(method="POST", url=url, data=chunk_data, headers=headers)
        if res.status_code == 200:
            return res
        else:
            tries = tries + 1

    return res


def upload_compose_object_gs(sess, bucket, key, object_parts, data_size):
    """
    create compose object from object components
    the object component name is in the format of key-part_number

    Args:
        sess(session): google client session
        bucket(str): bucket name
        key(str): key
        object_parts(list(int)): list of object part number
        data_size(int): total data size of all object components

    Return:
        http.Response
    """
    url = "https://www.googleapis.com/storage/v1/b/{}/o/{}/compose".format(
        bucket, urllib.quote(key, safe="")
    )
    payload = {"destination": {"contentType": "application/octet-stream"}}
    L = []
    for part in object_parts:
        L.append({"name": part})
    payload["sourceObjects"] = L

    headers = {
        "Host": "www.googleapis.com",
        "Content-Type": "application/json",
        "Content-Length": str(data_size),
    }
    retries = 0
    while retries < RETRIES_NUM:
        res = sess.request(
            method="POST", url=url, data=json.dumps(payload), headers=headers
        )
        if res.status_code == 200:
            return res
        else:
            retries += 1
    return None


def finish_multipart_upload_gs(sess, bucket, key, chunk_sizes):
    """
    concaternate all object parts

    Args:
        sess(session): google client session
        bucket(str): bucket name
        key(str): key
        chuck_sizes(list(int)): list of chunk sizes

    Return:
        http.Response
    """
    part_number = len(chunk_sizes)
    chunk_queue = queue.Queue()

    for i in range(0, part_number):
        chunk_queue.put({"key": key + "-" + str(i + 1), "size": chunk_sizes[i]})

    num = 0
    L = []
    total_size = 0

    while num < 32 and not chunk_queue.empty():
        chunk_info = chunk_queue.get()
        L.append(chunk_info["key"])
        total_size += chunk_info["size"]
        num +=1
    res = upload_compose_object_gs(sess, bucket, key, L, total_size)

    if res is not None:
        while not chunk_queue.empty():
            L = [key]
            num = 1
            while num < 32 and not chunk_queue.empty():
                chunk_info = chunk_queue.get()
                L.append(chunk_info["key"])
                total_size += chunk_info["size"]
                num +=1
            if len(L) > 1:
                res = upload_compose_object_gs(sess, bucket, key, L, total_size)
                if res is None:
                    break
    for i in range(0, part_number):
        component = key + "-" + str(i + 1)
        delete_object(sess, bucket, component)

    return res


def stream_object_from_gdc_api(fi, target_bucket, global_config, endpoint=None):
    """
    Stream object from gdc api. In order to check the integrity, we need to compute md5 during streaming data from
    gdc api and compute its local crc32c since google only provides crc32c for multi-part uploaded object.

    Args:
        fi(dict): object info
        target_bucket(str): target bucket

    Returns:
        None
    """

    class ThreadControl(object):
        def __init__(self):
            self.mutexLock = threading.Lock()
            self.sig_update_turn = 1

    def _handler(chunk_info):
        tries = 0
        request_success = False

        chunk = None
        while tries < RETRIES_NUM and not request_success:
            try:
                req = urllib2.Request(
                    data_endpoint,
                    headers={
                        "X-Auth-Token": GDC_TOKEN,
                        "Range": "bytes={}-{}".format(
                            chunk_info["start"], chunk_info["end"]
                        ),
                    },
                )

                chunk = urllib2.urlopen(req).read()
                if len(chunk) == chunk_info["end"] - chunk_info["start"] + 1:
                    request_success = True

            except urllib2.HTTPError as e:
                logger.warn(
                    "Fail to open http connection to gdc api. Take a sleep and retry. Detail {}".format(
                        e
                    )
                )
                time.sleep(5)
                tries += 1
            except SocketError as e:
                if e.errno != errno.ECONNRESET:
                    logger.warn(
                        "Connection reset. Take a sleep and retry. Detail {}".format(e)
                    )
                    time.sleep(20)
                    tries += 1
            except Exception as e:
                logger.warn(e)
                time.sleep(5)
                tries += 1

        if tries == RETRIES_NUM:
            raise Exception(
                "Can not open http connection to gdc api {}".format(data_endpoint)
            )

        md5 = hashlib.md5(chunk).digest()

        res = upload_chunk_to_gs(
            sess,
            chunk_data=chunk,
            bucket=target_bucket,
            key=object_path,
            part_number=chunk_info["part_number"],
        )
        if res.status_code != 200:
            raise Exception(
                "Can not upload chunk data of {} to {}".format(fi["id"], target_bucket)
            )

        while thead_control.sig_update_turn != chunk_info["part_number"]:
            time.sleep(1)

        thead_control.mutexLock.acquire()
        sig.update(chunk)
        crc32c.update(chunk)
        thead_control.sig_update_turn += 1
        thead_control.mutexLock.release()

        if thead_control.sig_update_turn % 10 == 0 and not global_config.get("quite"):
            logger.info(
                "Received {} MB".format(
                    thead_control.sig_update_turn * 1.0 / 1024 / 1024 * chunk_data_size
                )
            )

        return res.json(), chunk_info["part_number"], md5, len(chunk)

    thead_control = ThreadControl()
    thread_client = storage.Client()
    sess = AuthorizedSession(thread_client._credentials)

    object_path = "{}/{}".format(fi.get("id"), fi.get("file_name"))
    data_endpoint = endpoint or "https://api.gdc.cancer.gov/data/{}".format(
        fi.get("id")
    )

    sig = hashlib.md5()
    crc32c = crcmod.predefined.Crc('crc-32c')
    # prepare to compute local etag
    md5_digests = []

    chunk_data_size = global_config.get("data_chunk_size", 1024 * 1024 * 128)

    tasks = []
    for part_number, data_range in enumerate(
        generate_chunk_data_list(fi["size"], chunk_data_size)
    ):
        start, end = data_range
        tasks.append({"start": start, "end": end, "part_number": part_number + 1})

    pool = ThreadPool(global_config.get("multi_part_upload_threads", 10))
    results = pool.map(_handler, tasks)
    pool.close()
    pool.join()

    total_bytes_received = 0

    sorted_results = sorted(results, key=lambda x: x[1])

    chunk_sizes = []
    for res, part_number, md5, chunk_size in sorted_results:
        md5_digests.append(md5)
        total_bytes_received += chunk_size
        chunk_sizes.append(chunk_size)

    finish_multipart_upload_gs(
        sess=sess, bucket=target_bucket, key=object_path, chunk_sizes=chunk_sizes
    )

    sig_check_pass = validate_uploaded_data(
        fi, sess, target_bucket, sig, crc32c, sorted_results
    )

    if not sig_check_pass:
        delete_object(sess, target_bucket, object_path)
    else:
        logger.info(
            "successfully stream file {} to {}".format(object_path, target_bucket)
        )


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
                "Can not stream the object {}. Size does not match".format(fi.get("id"))
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
        res = get_object_metadata(sess, target_bucket, blob_name)
        if res.status_code == 200:
            logger.info(
                "{} is already copied ".format(fi["id"])
            )
            continue

        logger.info("Start streaming the object {}".format(fi["id"]))

        begin = timeit.default_timer()

        stream_object_from_gdc_api(fi, target_bucket, jobinfo.global_config)

        end = timeit.default_timer()

        logger.info("Throughput {}(MiB)/s".format(fi["size"]/(end-begin)/1000/1000))

    jobinfo.manager_ns.total_processed_files += len(files)
    logger.info(
        "{}/{} object are processed/copying ".format(
            jobinfo.manager_ns.total_processed_files, jobinfo.total_files
        )
    )

    return len(files)


def run(thread_num, global_config, job_name, manifest_file, bucket=None):
    """
    start threads and log after they finish
    """

    tasks, _ = prepare_data(manifest_file, global_config)

    manager = Manager()
    manager_ns = manager.Namespace()
    manager_ns.total_processed_files = 0

    jobInfos = []
    for task in tasks:
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

