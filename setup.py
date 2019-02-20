from setuptools import setup, find_packages
from os import path
# io.open is needed for projects that support Python 2.7
# It ensures open() defaults to text mode with universal newlines,
# and accepts an argument to specify the text encoding
# Python 3 only projects can skip this import
from io import open
import sbalance

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='slurm-sbalance',
    version=sbalance.__version__,  
    description='Slurm utility for checking account balance',  
    long_description=long_description, 
    long_description_content_type='text/markdown', 
    url='https://github.com/puttsk/slurm-sbalance',  
    author='Putt Sakdhnagool',
    author_email='putt.sakdhnagool@nectec.or.th', 
    classifiers=[ 
        'Development Status :: 3 - Alpha',
        'Intended Audience :: System Administrators',
        'Topic :: System :: Systems Administration',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
    ],
    keywords='slurm', 
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),
    python_requires='>=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*, !=3.5.*, <4',
    entry_points={ 
        'console_scripts': [
            'sbalance=sbalance:main',
        ],
    },

    project_urls={ 
        'Bug Reports': 'https://github.com/puttsk/slurm-sbalance/issues',
        'Source': 'https://github.com/puttsk/slurm-sbalance/',
    },
)
