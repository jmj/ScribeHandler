#!/usr/bin/python

from distutils.core import setup

setup(
    name='ScribeHandler',
    author='Jeremy Jones',
    author_email='jeremyj@letifer.org',
    url='http://github.com/jmj/ScribeHandler',
    version='0.05',
    py_modules=['ScribeHandler'],
    license='GPLv2',
    long_description=open('README').read(),
    description='ScribeHandler is a simple proxy layer that works with the python standard logging module',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
    ],
    keywords=[
        'scribe', 'logging', 'scribed'
    ],
    download_url='http://pypi.python.org/pypi/ScribeHandler',
    platform='any',

    )
