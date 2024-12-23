from setuptools import find_packages, setup

setup(
    name="us_airlines",
    packages=find_packages(exclude=["us_airlines_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
