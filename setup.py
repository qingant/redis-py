'''
Neinei App API Server. https://bitbucket.org/neineiappgroup/neineiapp-apiserver

Neinei App API Server
~~~~~~~~~~~~~~~~~~~~~

API Server for serving data to clients.

'''

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.setuptools import setup, find_packages

import os
import sys

sys.path.insert(0, os.getcwd())
from package_metadata import p

with open('requirements.pip') as f:
    install_reqs = [line for line in f.read().split('\n') if line]

readme = open('README.rst').read()
history = open('CHANGES.rst').read()

setup(
    name=p.title,
    version=p.version,
    url='https://github.com/zonyitoo/redis-py',
    license=p.license,
    author=p.author,
    author_email=p.email,
    description=p.description,
    long_description=readme,
    packages=find_packages(),
    include_package_data=True,
    install_requires=install_reqs,
    test_suite='redis.testsuite',
    zip_safe=False,
    keywords=p.title,
    entry_points={
        'console_scripts': [
            'redis-server=redis.server.servermain:server_main'
        ]
    },
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Operating System :: POSIX',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.4',
    ]
)
