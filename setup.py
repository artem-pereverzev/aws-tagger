import os
from setuptools import setup, find_packages
import warnings

setup(
    name='aws-tagger',
    version='0.6.3',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'boto3>=1.24.20',
        'botocore>=1.27.20',
        'click>=8.1.3',
        'docutils>=0.18.1',
        'futures3>=1.0.0',
        'jmespath>=1.0.1',
        'retrying>=1.3.3',
        's3transfer>=0.6.0',
        'six>=1.16.0'
    ],
    entry_points={
        "console_scripts": [
            "aws-tagger=tagger.cli:cli",
        ]
    },
    author="Patrick Cullen and the WaPo platform tools team",
    author_email="opensource@washingtonpost.com",
    url="https://github.com/washingtonpost/aws-tagger",
    download_url = "https://github.com/washingtonpost/aws-tagger/tarball/v0.6.3",
    keywords = ['tag', 'tagger', 'tagging', 'aws'],
    classifiers = []
)
