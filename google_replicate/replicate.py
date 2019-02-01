import timeit
import argparse
import json


def parse_arguments():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="action", dest="action")

    google_replicate_cmd = subparsers.add_parser("google_replicate")
    google_replicate_cmd.add_argument("--global_config", required=True)
    google_replicate_cmd.add_argument("--manifest_file", required=True)
    google_replicate_cmd.add_argument("--thread_num", required=True)

    google_indexing_cmd = subparsers.add_parser("indexing")

    # set config in dictionary.:
    # {
    #     "chunk_size": 100, # number of objects will be processed in single process/thread
    #     "log_bucket": "bucketname".
    #     "mode": "process|thread", # multiple process or multiple thread. Default: thread
    #     "quite": 1|0, # specify if we want to print all the logs or not. Default: 0
    #     "data_chunk_size": 1024 * 1024 * 128, # chunk size with multipart download and upload. Default 1024 * 1024 * 128
    #     "multi_part_upload_threads": 10, # Number of threads for multiple download and upload. Default 10
    # }
    google_indexing_cmd.add_argument("--global_config", required=True)
    google_indexing_cmd.add_argument("--manifest_file", required=True)
    google_indexing_cmd.add_argument("--thread_num", required=True)

    return parser.parse_args()


if __name__ == "__main__":
    start = timeit.default_timer()

    args = parse_arguments()
    if args.action == "google_replicate" or args.action == "indexing":
        job_name = "copying" if args.action == "google_replicate" else "indexing"
        import google_replicate

        google_replicate.run(
            int(args.thread_num),
            json.loads(args.global_config),
            job_name,
            args.manifest_file,
            None,
        )

    end = timeit.default_timer()
    print("Total time: {} seconds".format(end - start))

