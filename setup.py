from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("VERSION", "r") as fh:
    version = fh.read().strip()

setup(
    name='bkp-sync',
    version=version,
    url='http://github.com/jpfxgood/bkp',
    author="James Goodwin",
    author_email="bkp-sync@jlgoodwin.com",
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    description='A set of modules and scripts for backing up and syncing files',
    long_description_content_type='text/markdown',
    long_description=long_description,
    license = 'MIT',
    keywords= [
        'backup',
        'sync',
    ],
    install_requires=[
        's3cmd',
        'paramiko',
    ],
    scripts=[
        'scripts/bkp',
        'scripts/collect',
        'scripts/rstr',
        'scripts/setups3',
        'scripts/sync',
    ],
    packages=[
        'bkp_core',
    ],
    python_requires='>=3.6',
)
