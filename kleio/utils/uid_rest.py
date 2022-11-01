import numpy as np
import requests
from kleio import config


def get_next_id() -> np.uint64:
    try:
        response = requests.post(config.UNIQUE_ID_API_URL)
        j_string = response.json()
        print(f" Session ID: {j_string}")
        session = np.uint64(j_string)
        return session
    except Exception as err:
        print(f"ERROR: Can't get session id {err}")
        raise
