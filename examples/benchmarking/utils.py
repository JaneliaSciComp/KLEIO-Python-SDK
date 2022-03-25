def format_tuple(tu):
    return str(tu).replace("(", "").replace(")", "").replace(",", "-").replace(" ", "")


import os
from sys import platform as _platform


def empty_trash():
    """Empty trash folder.

    """
    text = "[tl] Empty the trash"
    if _platform == "linux" or _platform == "linux2":
        print('linux: %s' % text)
        os.system("rm -rf ~/.local/share/Trash/*")
    elif _platform == "darwin":
        print('OS X: %s' % text)
        os.system("sudo rm -rf ~/.Trash/*")
    elif _platform == "win32":
        print('Windows: %s' % text)
        try:
            os.system("rd /s c:\$Recycle.Bin")  # Windows 7 or Server 2008
        except:
            pass
        try:
            os.system("rd /s c:\recycler")  # Windows XP, Vista, or Server 2003
        except:
            pass
    else:
        print(_platform)

    # def add_size_bench(size_benchmark):
    #     tx = time.time()
    #     size_b = SizeBenchmark()
    #     t1 = time.time()
    #     used, available = data.get_df_used_remaining()
    #     df_time = time.time() - t1
    #     size_b.add(Remaining_space, available)
    #     size_b.add(Used_Size_df, used)
    #     t1 = time.time()
    #     size = data.get_size()
    #     size_time = time.time() - t1
    #     size_b.add(Logic_Size, size)
    #     t1 = time.time()
    #     du_size = data.du_size()
    #     du_time = time.time() - t1
    #     size_b.add(DU_Size, du_size)
    #     t1 = time.time()
    #     size_benchmark.write_line(size_b.format())
    #     print("df: {} - size: {} - du: {} - write: {} - total: {}".format(df_time,size_time,du_time,time.time()-t1,time.time()-tx))

    # def add_size_bench(size_benchmark):
    #     size_b = SizeBenchmark()
    #     used, available = data.get_df_used_remaining()
    #     size_b.add(Remaining_space, available)
    #     size_b.add(Used_Size_df, used)
    #     size = data.get_size()
    #     size_b.add(Logic_Size, size)
    #     du_size = data.du_size()
    #     size_b.add(DU_Size, du_size)
    #     size_benchmark.write_line(size_b.format())
