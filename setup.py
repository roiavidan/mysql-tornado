# -*- coding: utf-8 -*-

import os
from distutils.core import setup

setup(
    name='mysql-tornado',
    version='0.0.5',
    description=('Thread-based MySQLdb asynchronous wrapper for Tornado'),
    author='Roi Avidan',
    author_email='roi.avidan@terra.com.br',
    url='http://github.com/roiavidan/mysql-tornado',
    packages=['mysqltornado'],
    install_requires=['tornado>=3.2', 'MySQL-python>=1.2.5'],
    platforms=['any']
)

