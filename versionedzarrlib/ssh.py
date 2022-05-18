from paramiko import SSHClient
import base64
from scp import SCPClient, SCPException


class RemoteClient:
    """Client to interact with a remote host via SSH & SCP."""

    def __init__(self, host, user, password, remote_path):
        self.host = host
        self.user = user
        self.password = password
        self.remote_path = remote_path

    @property
    def connection(self):
        try:
            client = SSHClient()
            client.load_system_host_keys()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            client.connect(hostname=self.host,
                           username=self.user,
                           password=self.password,
                           look_for_keys=False,
                           )
            return client
        except Exception as e:
            print(f"Error connection: {e}")
            raise e

    @property
    def scp(self) -> SCPClient:
        conn = self.connection
        return SCPClient(conn.get_transport())

    def download_file(self, file: str):
        """Download file from remote host."""
        self.scp.get(file)

    def bulk_upload(self, files: [str]):
        """
        Upload multiple files to a remote directory.

        :param files: List of local files to be uploaded.
        :type files: List[str]
        """
        try:
            self.scp.put(files, remote_path=self.remote_path)
            print(
                f"Finished uploading {len(files)} files to {self.remote_path} on {self.host}"
            )
        except SCPException as e:
            raise e

    def execute_commands(self, commands: [str]):
        """
        Execute multiple commands in succession.
        :param commands: List of unix commands as strings.
        """
        for cmd in commands:
            stdin, stdout, stderr = self.client.exec_command(cmd)
            stdout.channel.recv_exit_status()
            response = stdout.readlines()
            for line in response:
                print(f"INPUT: {cmd} | OUTPUT: {line}")

    def disconnect(self):
        """Close ssh connection."""
        if self.client:
            self.client.close()
        if self.scp:
            self.scp.close()


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
