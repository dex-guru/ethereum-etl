import os

from setuptools import find_packages, setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


long_description = read("README.md") if os.path.isfile("README.md") else ""

setup(
    name="ethereum-etl",
    version="1.0",
    author="Evgeny Medvedev",
    author_email="evge.medvedev@gmail.com",
    description="Tools for exporting Ethereum blockchain data to CSV or JSON",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/blockchain-etl/ethereum-etl",
    packages=find_packages(exclude=["schemas", "tests*", "db", "helm"]),
    package_data={
        "blockchainetl.jobs.exporters": ["clickhouse_schemas.sql.tpl"],
        "ethereumetl": ["dex/*.json", "chains_config.json"],
    },
    include_package_data=True,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.10",
    ],
    keywords="ethereum",
    python_requires=">=3.10,<4",
    install_requires=read("requirements.txt").strip().split("\n"),
    entry_points={
        "console_scripts": [
            "ethereumetl=ethereumetl.cli:cli",
        ],
    },
    project_urls={
        "Bug Reports": "https://github.com/blockchain-etl/ethereum-etl/issues",
        "Chat": "https://gitter.im/ethereum-etl/Lobby",
        "Source": "https://github.com/blockchain-etl/ethereum-etl",
    },
)
