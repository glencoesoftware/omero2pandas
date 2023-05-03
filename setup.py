# encoding: utf-8
#
# Copyright (c) 2023 Glencoe Software, Inc. All rights reserved.
#
# This software is distributed under the terms described by the LICENCE file
# you can find at the root of the distribution bundle.
# If the file is missing please request a copy by contacting
# support@glencoesoftware.com.

from setuptools import setup
import version

setup(
    name='omero2pandas',
    version=version.getVersion(),
    packages=['omero2pandas'],
    url='https://github.com/glencoesoftware/omero2pandas',
    license='(c) Glencoe Software, Inc.',
    author='Glencoe Software, Inc.',
    author_email='info@glencoesoftware.com',
    description='OMERO.tables to pandas bridge',
    install_requires=[
        'omero-py',
        'pandas',
        'tqdm',
    ],
    setup_requires=['pytest-runner'],
    tests_require=['pytest'],
)
