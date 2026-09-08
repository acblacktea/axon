from setuptools import setup, find_packages

setup(
    name="axon_market_data",
    version="0.1.27",
    packages=find_packages(),
    install_requires=[
        "aiohttp>=3.8.0",
        "pyzmq",
        "pyyaml",
        "prometheus_client>=0.16.0",
        "psutil>=5.9.0",
    ],
    python_requires=">=3.8",
    author="Yang Liu",
    author_email="acblacktea@outlook.com",
    description="Axon market data service for cryptocurrency options trading",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
)
