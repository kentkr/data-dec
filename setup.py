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
    install_requires = [
        'databricks-connect==13.3.0',
        'pyspark==3.5.2',
        'setuptools==72.2.0',
        'pyyaml==6.0.2',
    ]
)
