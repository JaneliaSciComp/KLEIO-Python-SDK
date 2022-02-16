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
