import argparse
import technorati
import fetch_feeds
import multiprocessing

"""
TO DO:
1. Add switches to control what executes and what does not.
2. Divide workflow into more functions.
3. Use database JOINs rather than having to reconcile multiple dicts.
"""

def process_arguments():
    cores = 4 * multiprocessing.cpu_count()
    parser = argparse.ArgumentParser(description='Runs the Technorati crawler.')

    group = parser.add_mutually_exclusive_group(required=True)
    # If running in local mode, automatically make this the master.
    group.add_argument("-m", "--master", action="store_true", dest="MASTER",
        default=False,
        help="specify to host the Redis queue on this node and do initial"
        "processing.")
    group.add_argument('-w', "--master-ip", dest="MASTER_IP", 
      help="Run in worker mode and provide the IP address of the master node" 
      "hosting the Redis queue.")
    group.add_argument('-l', "--local", action="store_true", dest="LOCAL", 
      default=False, help="Run in local mode.")

    group3 = parser.add_argument_group("Parallelism")
    group3.add_argument("-c", "--cores", dest="cores", action='store',
      help="the number of cores to use per worker (or locally).", 
      default=cores, nargs=1)

    args = parser.parse_args()
    args.DISTRIBUTED = args.MASTER or args.MASTER_IP
    return args

def main():
    args = process_arguments()
    if args.MASTER or not args.DISTRIBUTED:
        tax = technorati.get_directory()
        technorati.get_directory_listings(tax)
        blogurls = fetch_feeds.harvest_urls()
        # TODO(ryan): Should be able to resume here.
        fetch_feeds.publish_to_redis(blogurls)
    fetch_feeds.probe_feeds(args.MASTER_IP or "127.0.0.1",
        args.cores, args.DISTRIBUTED)
    # fetch_feeds.reconcile()
    # fetch_feeds.extract_titles()
    # sanitizing_modeling.sanitize("/home/ryan/Documents/STOPWORDS",
    #"/home/ryan/dissertation/technorati_docs.csv")

if __name__ == "__main__":
    main()
