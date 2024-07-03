from setuptools import setup, find_packages #
from typing import List
import os


HYPHEN_E_DOT = '-e .'

def get_requirements(file_path: str) -> List[str]:
    requirements = []
    with open(file_path, 'r') as f:
        requirements = f.readlines()
    requirements = [req.replace("\n", "") for req in requirements]
    if HYPHEN_E_DOT in requirements:
        requirements.remove(HYPHEN_E_DOT)
    return requirements

# Get the directory where the setup.py file is located
this_directory = os.path.abspath(os.path.dirname(__file__))

# Open the README.md file relative to the setup.py file location
with open(os.path.join(this_directory, 'README.md'), 'r', encoding='utf-8') as f:
    long_description = f.read()


__version__ = "0.0.1"
REPO_NAME = "database-connector" 
PKG_NAME= "dbLinkPro" #for pypi 
AUTHOR_USER_NAME = "snehsuresh"  
AUTHOR_EMAIL = "snehsuresh02@gmail.com" 

setup(
    name=PKG_NAME,  #name of package
    version=__version__,    #version
    author=AUTHOR_USER_NAME,    #author 
    author_email=AUTHOR_EMAIL,  #
    description="A python package for connecting with database.",
    long_description=long_description, 
    long_description_content="text/markdown",
    url=f"https://github.com/{AUTHOR_USER_NAME}/{REPO_NAME}",
    project_urls={
        "Bug Tracker": f"https://github.com/{AUTHOR_USER_NAME}/{REPO_NAME}/issues",
    },
    package_dir={"": "src"},    #package from src forlder
    packages=find_packages(where="src"),
    install_requires=["pymongo", "pymongo[srv]", "dnspython", "pandas", "numpy", "ensure", "pytest", "mysql-connector-python"]
    )

