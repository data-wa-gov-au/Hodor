from setuptools import setup, find_packages

setup(
    name='Hodor',
    version='0.3',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'Click==2.4',
        'pyopenssl',
        'pycrypto',
        'google-api-python-client',
        'python-gflags',
        'httplib2',
        'pprintpp',
        'multiprocessing',
        'shapely',
        'tablib'
    ],
    entry_points='''
        [console_scripts]
        hodor=hodor.cli:cli
    ''',
)
