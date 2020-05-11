from glob import glob
import os
from setuptools import setup, find_packages

from txaws import version

def parse_requirements(filename):
    """load requirements from a pip requirements file"""
    lineiter = (line.strip() for line in open(filename))
    return [line for line in lineiter if line and (not line.startswith("#") and not line.startswith('-'))]

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

long_description = """
Twisted-based Asynchronous Libraries for Amazon Web Services and Eucalyptus
private clouds This project's goal is to have a complete Twisted API
representing the spectrum of Amazon's web services as well as support for
Eucalyptus clouds.
"""


setup(
    name="txAWS",
    version=version.txaws,
    description="Async library for EC2, OpenStack, and Eucalyptus",
    author="txAWS Developers",
    url="https://github.com/twisted/txaws",
    license="MIT",
    packages=find_packages(),
    scripts=glob("./bin/*"),
    long_description=long_description,
    install_requires = parse_requirements('requirements.txt'),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "Intended Audience :: Information Technology",
        "Programming Language :: Python",
        "Topic :: Database",
        "Topic :: Internet :: WWW/HTTP",
        "License :: OSI Approved :: MIT License",
       ],
    include_package_data=True,
    zip_safe=False,
    extras_require={
        "dev": ["treq", "zope.datetime", "boto3"],
    },
    )
