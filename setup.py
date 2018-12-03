from setuptools import setup, find_packages

setup(name='mantaray-client',
      version='1.0.0',
      packages=find_packages(),
      install_requires=['requests>=2.18.3',
                        'websocket_client',
                        'colorama'],
      entry_points={'console_scripts': [
          'mwa_client = mantaray.scripts.mwa_client:main']
      })
