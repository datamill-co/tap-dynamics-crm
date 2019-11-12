#!/usr/bin/env python

from setuptools import setup

setup(
    name="tap-dynamics",
    version="0.1",
    description="Singer.io tap for extracting data from the Microsoft Dynamics 365 API",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    install_requires=["requests==2.22.0", "singer-python==5.8.1", "odata"],
    dependency_links=["git://github.com/tuomur/python-odata.git#egg=odata"],
    entry_points="""
          [console_scripts]
          tap-dynamics=tap_dynamics:main
      """,
    packages=["tap_dynamics"],
)
