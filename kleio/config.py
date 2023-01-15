UNIQUE_ID_API_URL = "http://c13u06.int.janelia.org:8000/v1/id"
__version__ = "0.3.0"
TEST = True

test_value = 10


def get_test_value():
    global test_value
    test_value = test_value + 1
    return test_value
