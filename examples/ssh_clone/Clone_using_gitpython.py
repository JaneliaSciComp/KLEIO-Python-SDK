from getpass import getpass
from git import Repo

username = getpass(prompt='Janelia username: ')

password = getpass()

ip = 'c13u06.int.janelia.org'
repo = "/groups/scicompsoft/home/zouinkhim/test_git"
local_repo = "/Users/zouinkhim/Desktop/versioned_data/test"

try:
    ssh_command = f"sshpass -p {password} ssh -l {username}"
    repo = Repo.clone_from(f"ssh://{ip}:{repo}",
                           local_repo, env={"GIT_SSH_COMMAND": ssh_command})
except Exception as e:
    print("Error!")
    print(e)
else:
    print("Repo cloned successfully!")
