from setuptools import setup

setup(
    name='Hodor',
    version='0.1',
    packages=['hodor', 'hodor.commands'],
    include_package_data=True,
    install_requires=[
        'Click',
        'google-api-python-client',
        'python-gflags',
        'httplib2'
    ],
    entry_points='''
        [console_scripts]
        hodor=hodor.cli:cli
    ''',
)
