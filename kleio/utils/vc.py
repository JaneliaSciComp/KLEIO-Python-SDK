import os
import time

from git import Repo, InvalidGitRepositoryError, NoSuchPathError

from .exceptions import InvalidCompressionIndexError
from .version import Version


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
        self._repo = None
        self.current_version = None

    @property
    def repo(self):
        if self._repo is None:
            self._repo = Repo(self._path)
            self.current_version = Version(self._path)
        return self._repo

    def is_git_repo(self):
        try:
            _ = self.repo.git_dir
            return True
        except InvalidGitRepositoryError:
            return False

    def untracked_files(self):
        return self.repo.untracked_files

    def init_repo(self):
        """Initialize a vcs repository at the given path if specified

        :return: ``vc.VCS`` (the newly created version control)"""
        if not os.path.exists(self._path):
            raise NoSuchPathError(self._path)
        repo = Repo.init(self._path)
        with repo.config_writer() as cw:
            cw.set("core", "compression", "0")
            # https://github.com/git/git/blob/v2.3.0/Documentation/config.txt#L2155
            # From https://stackoverflow.com/a/28383598/3602294
            # Enable git push to this branch
            if not cw.has_section("receive"):
                cw.add_section("receive")
            # because server git version is 1.8.0
            cw.set("receive", "denyCurrentBranch", "false")
            # for newer git version use (2.3.6)
            # cw.set("receive", "denyCurrentBranch", "updateInstead")
        self.current_version = Version(self._path, create=True)
        # self.commit('Initial commit')

    def add_all(self):
        """stage all local changes to git index"""
        self.repo.git.add(all=True)

    def add(self, files: [str]):
        """add files to git index
        :param files:
            the files to add to vcs index :

                VCS("/Users/zouinkhim/dataset").add(["test/file.txt"]).
                """
        self.repo.index.add(files)

    def commit(self, message: str):
        """commit staged changes , need add() before
        :param message: commit message. """
        self.repo.index.commit(message)
        self.current_version.increment()

    def commit_all(self, message: str = None):
        """commit staged changes , need add() before
        :param message: commit message. """
        if message is None:
            files = self.untracked_files()
            message = "-".join(files)
        self.repo.git.add(all=True)
        self.repo.index.commit(message)
        self.current_version.increment()

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

    @classmethod
    def make_bare(cls, path):
        repo = Repo(path)
        with repo.config_writer() as cw:
            cw.set("core", "bare", "true")

