from setuptools import setup, find_packages

setup(name='mantaray-client',
      version='0.1.0',
      packages=['mantaray'],
      install_requires=['requests',
                        'websocket_client',
                        'colorama'],
      entry_points={'console_scripts': [
          'mwa_job_client = mantaray.scripts.mwa_client:main']
      })