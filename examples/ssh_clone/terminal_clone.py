import subprocess
from getpass import getpass

user = "zouinkhim"
host = "c13u06.int.janelia.org"
path = "/groups/scicompsoft/home/zouinkhim/test_git"
command = f"sshpass -p {getpass()} git clone -q {user}@{host}:{path}"

process = subprocess.Popen(command.split(" "), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
out, err = process.communicate()
print('out = {}\nerr = {}'.format(out, err))
