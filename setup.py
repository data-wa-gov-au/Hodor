from setuptools import setup

setup(
    name='Hodor',
    version='0.2',
    packages=['hodor', 'hodor.commands'],
    include_package_data=True,
    scripts=['hodor/retries.py'],
    install_requires=[
        'Click',
        'google-api-python-client',
        'python-gflags',
        'pyOpenSSL',
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
