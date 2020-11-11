import setuptools


__packagename__ = 'ondemand-dask'

setuptools.setup(
    name = __packagename__,
    packages = setuptools.find_packages(),
    include_package_data = True,
    version = '0.0.10',
    python_requires = '>=3.6.*',
    description = 'Dask cluster on demand and automatically delete itself after expired. Only support GCP for now.',
    author = 'huseinzol05',
    author_email = 'husein.zol05@gmail.com',
    url = 'https://github.com/kfit-dev/ondemand-dask',
    install_requires = [
        'requests',
        'herpetologist',
        'google-cloud-storage',
        'google-api-python-client',
        'cloudpickle',
    ],
    license = 'MIT',
    classifiers = [
        'Programming Language :: Python :: 3.7',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)
