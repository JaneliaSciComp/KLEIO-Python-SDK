import os
import time

Writing_index_time = 0
Commit_time = 1
Checkout_time = 2
GC_time = 3
Random_fill = 4
Initial_commit = 5
Initial_GC = 6

Remaining_space = 10
DU_Size = 11
Used_Size_df = 12

TIME_ELEMENTS = [
    "Writing_index_time",
    "Commit_time",
    "Checkout_time",
    "GC_time",
    "Random_fill",
    "Initial_commit",
    "Initial_GC"
]

SIZE_ELEMENTS = [
    "Remaining_space",
    "DU_Size",
    "Used_Size_df"
]

Type_Time = 0
Type_size = 1


class TimeBenchmark:
    def __init__(self, index) -> None:
        self.index = index
        self.elms = {}
        self.current = None
        self.start = None

    def format(self):
        elements = [""] * len(TIME_ELEMENTS)
        for k in self.elms.keys():
            elements[k] = str(self.elms[k])
        return str(self.index) + ";" + ";".join(elements)

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
        return "index;" + ";".join(TIME_ELEMENTS)


class SizeBenchmark:
    def __init__(self, index) -> None:
        self.index = index
        self.elms = {}

    def format(self):
        elements = [""] * len(SIZE_ELEMENTS)
        for k in self.elms.keys():
            elements[k - 10] = str(self.elms[k])
        return str(self.index) + ";" + ";".join(elements)

    def add(self, elm_type, val):
        self.elms[elm_type] = val

    @classmethod
    def get_header(cls):
        return "index;" + ";".join(SIZE_ELEMENTS)


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
        open(self.path, "a").write(line + "\n")
