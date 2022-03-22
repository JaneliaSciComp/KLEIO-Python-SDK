import os
import time

Reading_time = 0
Writing_index_time = 1
Get_new_index_time = 2
Write_raw_data_time = 3
Commit_time = 4
Checkout_time = 5
GC_time = 6

Remaining_space = 10
Logic_Size = 11
# DU_Size = 12
Used_Size_df = 12

TIME_ELEMENTS = [
    "Reading_time",
    "Writing_index_time",
    "Get_new_index_time",
    "Write_raw_data_time",
    "Commit_time",
    "Checkout_time",
    "GC_time"
]

SIZE_ELEMENTS = [
    "Remaining_space",
    "Logic_Size",
    # "DU_Size",
    "Used_Size_df"
]

Type_Time = 0
Type_size = 1


class TimeBenchmark:
    def __init__(self) -> None:
        self.elms = {}
        self.current = None
        self.start = None

    def format(self):
        elements = [""] * len(TIME_ELEMENTS)
        for k in self.elms.keys():
            elements[k] = str(self.elms[k])
        return ";".join(elements)

    def add_element(self, n: int):
        self.elms[n] = time.time()

    def start_element(self, n: int):
        self.start = time.time()
        self.current = n

    def done_element(self):
        ts = time.time() - self.start
        self.elms[self.current] = ts

    @classmethod
    def get_header(cls):
        return ";".join(TIME_ELEMENTS)


class SizeBenchmark:
    def __init__(self) -> None:
        self.elms = {}

    def format(self):
        elements = [""] * len(SIZE_ELEMENTS)
        for k in self.elms.keys():
            elements[k - 10] = str(self.elms[k])
        return ";".join(elements)

    def add(self, elm_type, val):
        self.elms[elm_type] = val

    @classmethod
    def get_header(cls):
        return ";".join(SIZE_ELEMENTS)


class Benchmarking(object):

    @staticmethod
    def get_ts():
        from datetime import datetime
        now = datetime.now()
        date_time = now.strftime("%Y%m%d_%H_%M_%S")
        return date_time

    @staticmethod
    def create_path(current_folder: str, elm_type: int, extra: str):
        st_type = ""
        if elm_type == Type_Time:
            st_type = "time_benchmark"
        elif elm_type == Type_size:
            st_type = "size_benchmark"

        return os.path.join(current_folder, "{}_{}_{}.csv".format(Benchmarking.get_ts(), st_type, extra))

    def __init__(self, path: str) -> None:
        self.path = path
        if os.path.exists(path):
            raise Exception("File exists already: " + path)

    def write_line(self, line):
        logger = open(self.path, "a")
        logger.write(line + "\n")
        logger.close()
