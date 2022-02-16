import time

from git import Repo


class GitInstance(object):

    def __init__(self, root_path: str):
        self.root_path = root_path

    def init(self):
        self.commit('Initial commit')

    def commit(self, commit_name: str):
        repo = Repo.init(self.root_path)
        repo.git.add(all=True)
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
