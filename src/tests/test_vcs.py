import unittest

from git import NoSuchPathError

from ..kleio.utils import VCS
from ..kleio.utils.exceptions import InvalidCompressionIndexError


class VCSTestCase(unittest.TestCase):
    def test(self):
        with self.assertRaises(InvalidCompressionIndexError):
            VCS("", -10)

        with self.assertRaises(NoSuchPathError):
            VCS("x").init_repo()


if __name__ == '__main__':
    unittest.main()
