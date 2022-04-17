import sys

sys.path.append('../')

from config import *
from path_config import *
from utils import *
from versionedzarrlib import *
import numpy as np
import dask.array as da
from tqdm import tqdm
import random




def main():
    # client = Client(processes=False)
    # print(client)
    # print(client.dashboard_link)


    total = 1
    for i in dims:
        total = total * i

    elms = np.arange(start=total, stop=total * 2, dtype=np.uint64)
    print("Got numpy array..")
    # np.random.shuffle(elms)
    # print("Array shuffled..")
    dask_data = da.from_array(elms)
    print("Dask created")
    dask_data = dask_data.reshape(dims)
    print("Array reshaped")
    print("Start rechunk")
    dask_data = dask_data.rechunk(index_chunk_size)
    print("rechunked")
    data = VersionedDataStore(path=data_path, shape=dims,raw_chunk_size=raw_chunk_size)



    data.create(overwrite=True)

    data.fill_index_dataset(data=dask_data)

    data.vc.init_repo()

    for i in tqdm(range(iterations)):
        pos = (
            random.randint(0, dims[0] - 1), random.randint(0, dims[1] - 1),
            random.randint(0, dims[2] - 1))
        index = data._get_next_index()
        data._update_index(index, pos)
        data.vc.add_all()
        data.vc.commit("Add {} at {}".format(index, pos))





if __name__ == "__main__":
    main()
