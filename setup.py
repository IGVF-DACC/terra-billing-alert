import os
import re
from pathlib import Path

import setuptools

META_PATH = Path('terra_billing_alert', '__init__.py')
HERE = os.path.abspath(os.path.dirname(__file__))


def read(*parts):
    """
    Build an absolute path from *parts* and and return the contents of the
    resulting file.  Assume UTF-8 encoding.
    """
    with Path(HERE, *parts).open(encoding='utf-8') as f:
        return f.read()


META_FILE = read(META_PATH)


def find_meta(meta):
    """
    Extract __*meta*__ from META_FILE.
    """
    meta_match = re.search(
        r"^__{meta}__ = ['\"]([^'\"]*)['\"]".format(meta=meta), META_FILE, re.M
    )
    if meta_match:
        return meta_match.group(1)
    raise


with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name='terra_billing_alert',
    version=find_meta('version'),
    python_requires='>=3.6',
    scripts=[
        'bin/terra_billing_alert',
    ],
    author='Jin wook Lee',
    author_email='leepc12@gmail.com',
    description='Terra Billing Alert script for Google Cloud Function',
    long_description='https://github.com/IGVF-DACC/terra-billing-alert',
    long_description_content_type='text/markdown',
    url='https://github.com/IGVF-DACC/terra-billing-alert',
    packages=setuptools.find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX :: Linux',
    ],
    install_requires=[
        'firecloud==0.16.32',
        'pandas==1.3.5',
        'pandas-gbq==0.17.5',
        'common==0.1.2',
        'slackclient==2.9.4',
        'google-cloud-bigquery',
    ],
)
