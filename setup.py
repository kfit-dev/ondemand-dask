import setuptools


__packagename__ = 'ondemand-dask'

setuptools.setup(
    name = __packagename__,
    packages = setuptools.find_packages(),
    version = '0.0.1',
    python_requires = '>=3.7.*',
    description = 'Dask cluster on demand and automatically delete itself after expired. Only support GCP for now.',
    author = 'huseinzol05',
    author_email = 'husein.zol05@gmail.com',
    url = 'https://github.com/huseinzol05/https://github.com/huseinzol05/ondemand-dask',
    install_requires = [
        'requests',
        'herpetologist',
        'google-cloud-storage',
        'google-api-python-client',
    ],
    license = 'MIT',
    classifiers = [
        'Programming Language :: Python :: 3.7',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    package_data = {'ondemand_dask': ['image', 'install.sh']},
)
