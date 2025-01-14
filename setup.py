from setuptools import setup, find_packages

setup(
    name="fpt-api",
    version="0.1.0",
    packages=find_packages(where="python"),
    package_dir={"": "python"},
    install_requires=[
        "shotgun_api3 @ git+https://github.com/shotgunsoftware/python-api.git",
        "requests",
        "urllib3",
        "certifi",
    ],
    python_requires=">=3.7",
)