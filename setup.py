import setuptools

setuptools.setup(name='realtimeaggregator',
      version='0.2.0',
      packages=setuptools.find_packages(exclude=['tests']),
      entry_points={
          'console_scripts': [
              'realtimeaggregator = realtimeaggregator.__main__:main',
              'realtimeaggregatorlogs = realtimeaggregator.logs.__main__:main'
          ]
      },
      )
