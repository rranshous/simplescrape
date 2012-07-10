from setuptools import setup
from setuptools.command.test import test as TestCommand
import dead_resources

requirements = [
    'BeautifulSoup',
    'requests'
]

setup(
    name="simplescrape",
    version=dead_resources.__version__,
    author="Robby Ranshous",
    author_email="rranshous@gmail.com",
    description="simple dead resource finder",
    keywords="scraping",
    url="https://github.com/rranshous/simplescrape",
    py_modules=["dead_resources"],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
    ],
    install_requires=requirements,
)
