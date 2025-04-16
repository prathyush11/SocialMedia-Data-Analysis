import logging
import requests
from requests.exceptions import RequestException, JSONDecodeError
import time

logger = logging.getLogger("reddit_client")
logger.propagate = False
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
sh.setFormatter(formatter)
logger.addHandler(sh)

class RedditClient:
    def __init__(self, client_id, client_secret, user_agent):
        self.client_id = client_id
        self.client_secret = client_secret
        self.user_agent = user_agent
        self.access_token = None

    def get_access_token(self):
        try:
            auth = requests.auth.HTTPBasicAuth(self.client_id, self.client_secret)
            data = {'grant_type': 'client_credentials'}
            # data = {'grant_type': 'password','password': 'echosocial'}
            # data = {'grant_type': 'password', 'username': 'Wrong-Season-4271', 'password': 'echosocial'}
            headers = {'User-Agent': self.user_agent}
            res = requests.post('https://www.reddit.com/api/v1/access_token', auth=auth, data=data, headers=headers)
            res.raise_for_status()
            token = res.json()['access_token']
            self.access_token = token
            return token
        except (RequestException, JSONDecodeError) as e:
            logger.error(f"Error getting access token: {e}")
            return None

    def get_posts(self, subreddit, limit=100, after=None):
        try:
            if not self.access_token:
                self.get_access_token()
            headers = {
                'Authorization': f'bearer {self.access_token}',
                'User-Agent': self.user_agent
            }
            params = {'limit': limit, 'after': after}
            url = f'https://oauth.reddit.com/r/{subreddit}/new'
            # print(headers)
            res = requests.get(url, headers=headers, params=params)
            res.raise_for_status()
            return res.json(), res
        except (RequestException, JSONDecodeError) as e:
            logger.error(f"Error getting posts from {subreddit}: {e}")
            return None

    