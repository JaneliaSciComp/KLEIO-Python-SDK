import time

from git import Repo

gitattributes = ".gitattributes"


class GitInstance(object):

    def __init__(self, root_path: str):
        self.root_path = root_path

    def create_attributes(self):
        with open(self.root_path, "w") as A:
            A.write("* binary\n")
            A.write("*.* binary\n")

    def init(self):
        Repo.init(self.root_path)
        # self.commit('Initial commit')

    def add(self):
        repo = Repo(self.root_path)
        repo.git.add(all=True)

    def commit(self, commit_name: str):
        repo = Repo(self.root_path)
        repo.index.commit(commit_name)

    def show_history(self):
        repo = Repo.init(self.root_path)
        commits = repo.iter_commits('--all')
        for commit in commits:
            print("Committed by %s on %s with sha %s" % (
                commit.committer.name, time.strftime("%a, %d %b %Y %H:%M", time.localtime(commit.committed_date)),
                commit.hexsha))

    def checkout_commit(self, commit_id):
        repo = Repo.init(self.root_path)
        repo.git.checkout(commit_id)

    def checkout_branch(self, branch_name: str, create: bool = False):
        repo = Repo.init(self.root_path)
        if create:
            print("Create new branch: {}".format(branch_name))
            repo.git.checkout('-b', branch_name)
        else:
            # print("Moved to branch: {}".format(branch_name))
            repo.git.checkout(branch_name)

    def gc(self):
        repo = Repo.init(self.root_path)
        repo.git.gc()
