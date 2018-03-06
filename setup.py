import setuptools

setuptools.setup(
    name='realtimeaggregator',
    version = '0.2.0',
    description = 'Software for aggregating realtime feeds',
    author = 'James Fennell',
    author_email = 'jamespfennell@fgmail.com',
    url = 'https://github.com/jamespfennell/realtime-aggregator',
    download_url = (
        'https://github.com/jamespfennell/realtime-aggregator/archive/0.2.0.tar.gz'
        ),
    packages=setuptools.find_packages(exclude=['*tests']),
    entry_points={
        'console_scripts': [
            'realtimeaggregator = realtimeaggregator.__main__:main',
            'realtimeaggregatorlogs = realtimeaggregator.logs.__main__:main'
            ]
        },
    install_requires=[
        'requests',
        'gtfs-realtime-bindings',
        'boto3'
        ],
    python_requires='>=3.5',
    )
