from setuptools import find_packages, setup
setup(
    name='Versionedstoragelib',
    packages=find_packages(),
    version='0.1.0',
    description='Versioned storage',
    author='Marwan Zouinkhi',
    license='MIT',
    install_requires=[],
    setup_requires=['pytest-runner'],
    tests_require=['pytest==6.2.5'],
    test_suite='tests',
)