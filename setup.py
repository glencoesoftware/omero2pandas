# encoding: utf-8
#
# Copyright (c) 2023 Glencoe Software, Inc. All rights reserved.
#
# This software is distributed under the terms described by the LICENCE file
# you can find at the root of the distribution bundle.
# If the file is missing please request a copy by contacting
# support@glencoesoftware.com.

import os

from setuptools import setup
import version


def read(fname):
    """
    Utility function to read the README file.
    :rtype : String
    """
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name='omero2pandas',
    python_requires='>=3.6',
    version=version.getVersion(),
    packages=['omero2pandas'],
    url='https://github.com/glencoesoftware/omero2pandas',
    author='Glencoe Software, Inc.',
    author_email='info@glencoesoftware.com',
    description='OMERO.tables to pandas bridge',
    long_description=read('README.md'),
    long_description_content_type='text/markdown',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Intended Audience :: Science/Research',
        'Intended Audience :: Developers',
        'Intended Audience :: End Users/Desktop',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10'
    ],
    keywords='',
    include_package_data=True,
    platforms='any',
    install_requires=[
        'omero-py',
        'pandas',
        'tqdm',
    ],
    extras_require={
            "token": ["omero-user-token>=0.3.0"],
        },
    setup_requires=['pytest-runner', 'flake8'],
    tests_require=['pytest', 'flake8'],
    zip_safe=True,
)
