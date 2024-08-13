from setuptools import setup, find_packages

setup(
    name='data_dec',
    version='0.0.1',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'dec=data_dec.cli:main',
        ],
    },
)
