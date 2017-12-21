from setuptools import setup, find_packages

setup(name='mantaray-client',
      version='0.1.0',
      packages=find_packages(),
      install_requires=['requests',
                        'websocket_client',
                        'colorama'],
      entry_points={'console_scripts': [
          'mwa_client = mantaray.scripts.mwa_client:main']
      })