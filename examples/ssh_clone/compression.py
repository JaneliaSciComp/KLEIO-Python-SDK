from numcodecs import GZip
import zarr

dims = (600, 600, 600)
chunks = (128, 128, 128)

# zarr.open("/Users/zouinkhim/Desktop/test_compress", shape=dims,
#           chunks=chunks, mode='w-',
#           dtype="u8", compression=GZip())



# # from numcodecs import GZip
# import zarr
#
# # dims = (600, 600, 600)
# # chunks = (128, 128, 128)
#
z = zarr.open("/Users/zouinkhim/Desktop/test_compress",mode='r')
print(z[0,0,0])

