from setuptools import setup, find_packages

setup(
    name="calais_order_execution",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "aiohttp>=3.9.0",
        "websockets>=12.0",
        "pyyaml>=6.0",
        "pyzmq>=25.0.0",
    ],
    python_requires=">=3.10",
    author="Yang Liu",
    author_email="acblacktea@outlook.com",
    description="Calais order execution service for cryptocurrency options trading",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
)
