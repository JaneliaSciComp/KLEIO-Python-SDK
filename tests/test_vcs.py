import unittest

from git import NoSuchPathError

from versionedzarrlib.VersionControl import VCS, InvalidCompressionIndexError


class VCSTestCase(unittest.TestCase):
    def test(self):
        with self.assertRaises(InvalidCompressionIndexError):
            VCS("", -10)

        with self.assertRaises(NoSuchPathError):
            VCS("x").init_repo()


if __name__ == '__main__':
    unittest.main()
