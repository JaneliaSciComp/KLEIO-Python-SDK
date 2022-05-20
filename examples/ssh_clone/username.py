# repo.config_writer().set_value("user", "name", "myusername").release()
# repo.config_writer().set_value("user", "email", "myemail").release()


import tempfile
import shutil

dirpath = tempfile.mkdtemp()
print(dirpath)
# ... do stuff with dirpath
shutil.rmtree(dirpath)