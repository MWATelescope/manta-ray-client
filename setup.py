from setuptools import setup, find_packages

setup(
    name="mantaray-client",
    version="2.0.1",
    packages=find_packages(),
    install_requires=["requests>=2.18.3", "websocket_client", "colorama"],
    entry_points={
        "console_scripts": ["mwa_client = mantaray.scripts.mwa_client:main"]
    },
)
