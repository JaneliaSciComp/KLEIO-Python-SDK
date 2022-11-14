# from kleio.meta import KleioMetadata
# tmp_file = '/Users/zouinkhim/Desktop/tmp/tmp.json'
# # f = KleioMetadata()
# # f.save(tmp_file)
#
# # print(dict(f))
# # x = f.to_json()
#
# # x2 = KleioMetadata.from_json(x)
# # print(x2)
#
# x = KleioMetadata.read_from_file(tmp_file)
# print(x)
# print(x._kleio_version)
# print(x._test)

import math


def get_nb_chunks(dims, chunks):
    return [math.ceil(d / c) for d, c in zip(dims, chunks)]


dims = [60, 60, 60]
chunk = [10, 10, 10]
print(get_nb_chunks(dims, chunk))
