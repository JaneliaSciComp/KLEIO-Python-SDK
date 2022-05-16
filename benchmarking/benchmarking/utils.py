import os
from sys import platform as _platform


def format_tuple(tu):
    return str(tu).replace("(", "").replace(")", "").replace(",", "-").replace(" ", "")


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
