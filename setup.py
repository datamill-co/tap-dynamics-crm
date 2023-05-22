#!/usr/bin/env python

from setuptools import setup

setup(name='tap-dynamics',
      version='0.0.1',
      description='Singer.io tap for extracting data from the Microsoft Dynamics 365 API',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_deputy'],
      install_requires=[
            'requests==2.31.0',
            'singer-python==5.8.1'
      ],
      entry_points='''
          [console_scripts]
          tap-dynamics-crm=tap_dynamics:main
      ''',
      packages=['tap_dynamics']
)
