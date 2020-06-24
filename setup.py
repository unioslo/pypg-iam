#!/usr/bin/env python3
# Authors:
# - Vincent Garonne <vgaronne@gmail.com>, 2020

from setuptools import setup

setup(
    name='pypg-iam',
    version='0.7.0',
    description='python library for pg-iam',
    author='Leon du Toit, Milen Kouylekov',
    author_email='dutoit.leon@gmail.com',
    url='https://github.com/leondutoit/pg-iam',
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.0'],
    install_requires=["sqlalchemy==1.3.6",
                      "psycopg2==2.8.3",
                      "sqlalchemy-utils==0.36.2"],
    packages=['iam', 'iam.database'],
    package_data={
        'iam': [
            'tests/*.py',
        ]
    },
)
