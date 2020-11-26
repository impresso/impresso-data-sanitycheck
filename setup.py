"""Config for Pypi."""

from setuptools import setup, find_packages

VERSION = "0.2.2"


DESCRIPTION = "Python module for data sanity check."

setup(
    name='sanity_check',
    author='Matteo Romanello, Maud Ehrmann',
    author_email='matteo.romanello@epfl.ch, maud.ehrmann@epfl.ch',
    url='https://github.com/impresso/impresso-data-sanitycheck',
    version=VERSION,
    packages=find_packages(),
    long_description=DESCRIPTION,
    python_requires='>=3.6',
    install_requires=[
        'docopt',
        'humanize',
        'aenum',
        'requests',
        'tqdm',
        'distributed',
        'dask[complete]',
        'seaborn',
        'tabulate',
        'dask-k8',
        'impresso-pycommons'
        'impresso-text-importer'
    ],
    dependency_links=[
        'https://github.com/impresso/impresso-master-db/tarball/master#egg=package-1.0'
    ]

)
