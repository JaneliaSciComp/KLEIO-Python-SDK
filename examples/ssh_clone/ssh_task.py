import paramiko
from getpass import getpass

ip = 'c13u06.int.janelia.org'
username = 'zouinkhim'
password = getpass()

commands = ['ls', 'pwd', 'llsdsd', 'who', 'hostname']

for command in commands:
    print(f"{'#' * 10} Executing the Command : {command} {'#' * 10}")
    stdin1, stdout1, stderr1 = session.exec_command(command)
    # time.sleep(.5)
    print(stdout1.read().decode())
    err = stderr1.read().decode()
    if err:
        print(err)

session.close()
