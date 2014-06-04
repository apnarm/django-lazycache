#!/usr/bin/env python

from setuptools import setup

setup(
    name='django-lazycache',
    version='0.0.1',
    description='Caching for Django models.',
    author='Raymond Butcher',
    author_email='randomy@gmail.com',
    url='https://github.com/apn-online/django-lazycache',
    license='MIT',
    packages=(
        'lazycache',
        'lazymodel',
    ),
    install_requires=(
        'django',
    ),
)
