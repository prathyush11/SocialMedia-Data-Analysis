from chan_client import ChanClient
import logging
from pyfaktory import Client, Consumer, Job, Producer
import datetime
import psycopg2
from psycopg2 import errors
# these three lines allow psycopg to insert a dict into
# a jsonb coloumn
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter

register_adapter(dict, Json)

# load in function for .env reading
from dotenv import load_dotenv


logger = logging.getLogger("4chan client")
logger.propagate = False
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)

load_dotenv()

import os

FAKTORY_SERVER_URL = os.environ.get("FAKTORY_SERVER_URL")
DATABASE_URL = os.environ.get("DATABASE_URL")

"""
Return all the thread numbers from a catalog json object
"""


def thread_numbers_from_catalog(catalog):
    thread_numbers = []
    for page in catalog:
        for thread in page["threads"]:
            thread_number = thread["no"]
            thread_numbers.append(thread_number)

    return thread_numbers


"""
Return thread numbers that existed in previous but don't exist
in current
"""


def find_dead_threads(previous_catalog_thread_numbers, current_catalog_thread_numbers):
    dead_thread_numbers = set(previous_catalog_thread_numbers).difference(
        set(current_catalog_thread_numbers)
    )
    return dead_thread_numbers


"""
Crawl a given thread and get its json.
Insert the posts into db
"""


def crawl_thread(board, thread_number):
    chan_client = ChanClient()
    thread_data = chan_client.get_thread(board, thread_number)

    # Check if thread_data is None
    if thread_data is None:
        logger.error(f"Failed to fetch thread data for {board}/{thread_number}")
        return  # Or handle the error differently
    
    logger.info(f"Thread: {board}/{thread_number}/:\n{thread_data}")

    try:
        conn = psycopg2.connect(dsn=DATABASE_URL)
        cur = conn.cursor()

        # Check if 'posts' key exists in thread_data
        if 'posts' not in thread_data:
            logger.error(f"'posts' key not found in thread_data for {board}/{thread_number}")
            return
        
        for post in thread_data["posts"]:
            post_number = post["no"]

            try:
                q = """
                    INSERT INTO posts (board, thread_number, post_number, data)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                """
                cur.execute(q, (board, thread_number, post_number, post))
                conn.commit()
                db_id = cur.fetchone()[0]
                logging.info(f"Inserted DB id: {db_id}")

            except errors.UniqueViolation as e:
                logger.warning(f"Duplicate post: {e}")  # Log and continue
                conn.rollback()  # Rollback the transaction on error
            except psycopg2.Error as e:
                logger.error(f"Database error: {e}")
                conn.rollback()
            except (NameError, KeyError, IndexError, TypeError, ValueError) as e:
                logger.error(f"Error processing post data: {e}")
                conn.rollback()

    except psycopg2.Error as e:
        logger.error(f"Database connection error: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()


"""
Go out, grab the catalog for a given board, and figure out what threads we need
to collect.

For each thread to collect, enqueue a new job to crawl the thread.

Schedule catalog crawl to run again at some point in the future.
"""


def crawl_catalog(board, previous_catalog_thread_numbers=[]):
    chan_client = ChanClient()

    current_catalog = chan_client.get_catalog(board)

    current_catalog_thread_numbers = thread_numbers_from_catalog(current_catalog)

    dead_threads = find_dead_threads(
        previous_catalog_thread_numbers, current_catalog_thread_numbers
    )
    logger.info(f"dead threads: {dead_threads}")

    # issue the crawl thread jobs for each dead thread
    crawl_thread_jobs = []
    with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
        producer = Producer(client=client)
        for dead_thread in dead_threads:
            # see https://github.com/ghilesmeddour/faktory_worker_python/blob/main/src/pyfaktory/models.py
            # what a `Job` looks like
            job = Job(
                jobtype="crawl-thread", args=(board, dead_thread), queue="crawl-thread"
            )

            crawl_thread_jobs.append(job)

        producer.push_bulk(crawl_thread_jobs)

    # Schedule another catalog crawl to happen at some point in future
    with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
        producer = Producer(client=client)
        # figure out how to use non depcreated methods on your own
        # run_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=5)
        run_at = datetime.datetime.utcnow() + datetime.timedelta(minutes=5)
        run_at = run_at.isoformat()[:-7] + "Z"
        logger.info(f"run_at = {run_at}")
        job = Job(
            jobtype="crawl-catalog",
            args=(board, current_catalog_thread_numbers),
            queue="crawl-catalog",
            at=str(run_at),
        )
        producer.push(job)


if __name__ == "__main__":
    # we want to pull jobs off the queues and execute them
    # FOREVER (continuously)
    with Client(faktory_url=FAKTORY_SERVER_URL, role="consumer") as client:
        consumer = Consumer(
            client=client, queues=["crawl-catalog", "crawl-thread"], concurrency=5
        )
        consumer.register("crawl-catalog", crawl_catalog)
        consumer.register("crawl-thread", crawl_thread)
        # tell the consumer to pull jobs off queue and execute them!
        consumer.run()
