import logging
from pyfaktory import Client, Consumer, Job, Producer
import time
import random
import sys

logger = logging.getLogger("faktory test")
logger.propagate = False
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)

if __name__ == "__main__":
    subreddits = [sys.argv[1], sys.argv[2]]
    print(f"Cold starting crawl for subreddits {subreddits}")

    faktory_server_url = "tcp://:password@localhost:7429"

    with Client(faktory_url=faktory_server_url, role="producer") as client:
        producer = Producer(client=client)
        for subreddit in subreddits:
            job = Job(jobtype="crawl-subreddit", args=(subreddit,), queue="crawl-subreddit")
            producer.push(job)
