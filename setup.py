#!/usr/bin/env python3

from setuptools import setup

setup(
    name='pypg-iam',
    version='0.2.0',
    description='python library for pg-iam',
    author='Leon du Toit',
    author_email='dutoit.leon@gmail.com',
    url='https://github.com/leondutoit/pg-iam',
    packages=['iam'],
    package_data={
        'iam': [
            'tests/*.py',
        ]
    },
    scripts=['scripts/test-iam']
)
