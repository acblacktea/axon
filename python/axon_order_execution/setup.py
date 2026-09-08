from pathlib import Path

from setuptools import setup, find_packages

# The README lives at the repo root (python/axon_order_execution/ -> up 3),
# because it documents both the Python and the C++ implementation.
_README = Path(__file__).resolve().parents[2] / "README.md"

setup(
    name="axon_order_execution",
    version="0.1.7",
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
    description="Axon order execution service for cryptocurrency options trading",
    long_description=_README.read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
)
