from setuptools import find_packages, setup

setup(
    name="music_rag_etl",
    packages=find_packages(exclude=["music_rag_etl_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
