import os
import time
from .exceptions import InvalidCompressionIndexError
from git import Repo, NoSuchPathError


class VCS(object):

    def __init__(self, path: str, compression=0):
        """Create a new VCS instance
        :param path:
            the path to either the root vcs directory, used for index matrix file :
        
                repo = VCS("/Users/zouinkhim/dataset").

        :param compression:
            git compression level, based on benchmarks, found that None compression is the fastest for commits
            can vary from 0 to 9. 9 is the slowest
        :return: ``vc.VCS`` """
        if not (0 <= compression <= 9):
            raise InvalidCompressionIndexError(compression)
        self._path = path
        self._compression = compression

    def init_repo(self):
        """Initialize a vcs repository at the given path if specified

        :return: ``vc.VCS`` (the newly created version control)"""
        if not os.path.exists(self._path):
            raise NoSuchPathError(self._path)
        repo = Repo.init(self._path)
        with repo.config_writer() as cw:
            cw.set("core", "compression", "0")

        # self.commit('Initial commit')

    def add_all(self):
        """stage all local changes to git index"""
        repo = Repo(self._path)
        repo.git.add(all=True)

    def add(self, files: [str]):
        """add files to git index
        :param files:
            the files to add to vcs index :

                VCS("/Users/zouinkhim/dataset").add(["test/file.txt"]).
                """
        repo = Repo(self._path)
        repo.index.add(files)

    def commit(self, message: str):
        """commit staged changes , need add() before
        :param message: commit message. """
        repo = Repo(self._path)
        repo.index.commit(message)

    def show_history(self):
        """ Show git history """
        repo = Repo.init(self._path)
        commits = repo.iter_commits('--all')
        for commit in commits:
            print("Committed by %s on %s with sha %s" % (
                commit.committer.name, time.strftime("%a, %d %b %Y %H:%M", time.localtime(commit.committed_date)),
                commit.hexsha))

    def checkout_commit(self, commit_id):
        """checkout repo to a specific commit id
        :param commit_id: commit id to stage on. """
        repo = Repo.init(self._path)
        repo.git.checkout(commit_id)

    def checkout_branch(self, branch_name: str, create: bool = False):
        """checkout repo to a different branch
        :param branch_name: The branch name
        :param create: Create a new branch or move to existent branch"""
        repo = Repo.init(self._path)
        if create:
            print("Create new branch: {}".format(branch_name))
            repo.git.checkout('-b', branch_name)
        else:
            # print("Moved to branch: {}".format(branch_name))
            repo.git.checkout(branch_name)

    def gc(self):
        """Collect garbage, to be run every while """
        repo = Repo.init(self._path)
        repo.git.gc()
