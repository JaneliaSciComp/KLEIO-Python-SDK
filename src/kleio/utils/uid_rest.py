import numpy as np
import requests
from kleio import config

from requests.exceptions import ConnectTimeout


def get_next_id() -> np.uint64:
    if config.TEST:
        return config.get_test_value()
    try:
        response = requests.post(config.UNIQUE_ID_API_URL, timeout=(2, 2))
        j_string = response.json()
        st_value = j_string['id']
        session = np.uint64(st_value)
        return session
    except ConnectTimeout as err:
        raise "ERROR: Can't get session id from server {}".format(err)
