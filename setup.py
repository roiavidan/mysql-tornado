# -*- coding: utf-8 -*-

import os
from distutils.core import setup

setup(
    name='mysql-tornado',
    version='0.0.1',
    description=('Thread-based MySQLdb asynchronous wrapper for Tornado'),
    author='Roi Avidan',
    author_email='roi.avidan@terra.com.br',
    url='http://github.com/roiavidan/mysql-tornado',
    packages=['mysqltornado'],
    license=open(os.path.join(os.path.dirname(__file__), 'LICENSE')).read(),
    install_requires=open(os.path.join(os.path.dirname(__file__), 'requirements.txt')).read(),
    platforms=['any']
)

