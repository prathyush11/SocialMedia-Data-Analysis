from reddit_client import RedditClient
import logging
from pyfaktory import Client, Consumer, Job, Producer
import datetime
import psycopg2
from psycopg2 import errors
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter
from dotenv import load_dotenv
from requests.exceptions import RequestException, JSONDecodeError
import os
import time
register_adapter(dict, Json)
load_dotenv()

logger = logging.getLogger("reddit_crawler")
logger.propagate = False
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)
# rerunner = None

FAKTORY_SERVER_URL = os.environ.get("FAKTORY_SERVER_URL")
DATABASE_URL = os.environ.get("DATABASE_URL")
REDDIT_CLIENT_ID = os.environ.get("REDDIT_CLIENT_ID")
REDDIT_CLIENT_SECRET = os.environ.get("REDDIT_CLIENT_SECRET")
REDDIT_USER_AGENT = os.environ.get("REDDIT_USER_AGENT")



def crawl_subreddit(subreddit, after=None):
    rerunner = None
    reddit_client = RedditClient(REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT)
    try:
        posts_data, res = reddit_client.get_posts(subreddit, after=after)
        # print("Headers: ", posts_data['headers'])
    except RequestException as e: 
        if e.response is not None and e.response.status_code == 429:
            # Handle 429 error (rate limit exceeded)
            retry_after = int(e.response.headers.get('retry-after', 60))
            logger.warning(f"Rate limited! Retrying in {retry_after} seconds.")
            time.sleep(retry_after)  # Wait as indicated by Retry-After header
            posts_data = reddit_client.get_posts(subreddit, after=after)  # Retry
        else:
            logger.error(f"Failed to fetch posts from {subreddit}: {e}")
            return

    if posts_data is None:
        logger.error(f"Failed to fetch posts from {subreddit}")
        return

    try:
        conn = psycopg2.connect(dsn=DATABASE_URL)
        cur = conn.cursor()

        for post in posts_data['data']['children']:
            post_data = post['data']
            post_id = post_data['id']

            try:
                q = """
                  INSERT INTO posts (subreddit, post_id, data)
                    VALUES (%s, %s, %s)
                    RETURNING id
                """
                cur.execute(q, (subreddit, post_id, post_data))
                conn.commit()
                db_id = cur.fetchone()[0]
                logger.info(f"Inserted DB id: {db_id}")

            except errors.UniqueViolation as e:
                logger.warning(f"Duplicate post: {e}")
                conn.rollback()
            except psycopg2.Error as e:
                logger.error(f"Database error: {e}")
                conn.rollback()
            except (NameError, KeyError, IndexError, TypeError, ValueError) as e:
                logger.error(f"Error processing post data: {e}")
                conn.rollback()

        

        new_after = posts_data['data']['after']
        logger.info(f"Next after value: {new_after}")  # Log new_after
        if new_after==None:
            print("I'm waiting")
            time.sleep(900)
            new_after= rerunner


        if new_after:
            rerunner=new_after
            remaining_requests = int(float(res.headers.get('x-ratelimit-remaining', 0)))
            if remaining_requests < 5:
                # Enter sleep mode to avoid hitting the limit
                reset_timestamp = int(float(posts_data['headers'].get('x-ratelimit-reset', 60)))
                sleep_time = reset_timestamp - int(time.time())
                logger.warning(f"Approaching rate limit! Sleeping for {sleep_time} seconds.")
                time.sleep(sleep_time)

            
            with Client(faktory_url=FAKTORY_SERVER_URL, role="producer") as client:
                producer = Producer(client=client)
                # run_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=1)
                run_at = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=2)
                run_at = run_at.isoformat()[:-7] + "Z"
                job = Job(
                    jobtype="crawl-subreddit",
                    args=(subreddit, new_after),
                    queue="crawl-subreddit",
                    at=str(run_at),
                )
                producer.push(job)
        


    except psycopg2.Error as e:
        logger.error(f"Database connection error: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()


if __name__ == "__main__":
    with Client(faktory_url=FAKTORY_SERVER_URL, role="consumer") as client:
        consumer = Consumer(client=client, queues=["crawl-subreddit"], concurrency=10)
        consumer.register("crawl-subreddit", crawl_subreddit)
        consumer.run()