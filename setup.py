from setuptools import find_packages, setup

setup(
    name="dagster_law",
    packages=find_packages(exclude=["dagster_law_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "duckdb",
        "aiohttp",
        "aiofiles"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
