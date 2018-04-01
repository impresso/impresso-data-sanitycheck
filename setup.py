"""Config for Pypi."""

import os
from setuptools import setup, find_packages

VERSION = "0.1.1"


DESCRIPTION = "Python module for data sanity check."

setup(
    name='sanity_check',
    author='Matteo Romanello, Maud Ehrmann',
    author_email='matteo.romanello@epfl.ch, maud.ehrmann@epfl.ch',
    url='https://github.com/impresso/impresso-data-sanitycheck',
    version=VERSION,
    packages=find_packages(),
    long_description=DESCRIPTION, install_requires=['docopt', 'humanize']
    # install_requires=[]
)