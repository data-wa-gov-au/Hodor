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
        'httplib2',
        'multiprocessing'
    ],
    entry_points='''
        [console_scripts]
        hodor=hodor.cli:cli
    ''',
)
