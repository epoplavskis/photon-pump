import io
import sys
import versioneer

from setuptools import setup
from setuptools.command.test import test as TestCommand


def read(*filenames, **kwargs):
    encoding = kwargs.get("encoding", "utf-8")
    sep = kwargs.get("sep", "\n")
    buf = []
    for filename in filenames:
        with io.open(filename, encoding=encoding) as f:
            buf.append(f.read())
    return sep.join(buf)


long_description = read("README.rst", "CHANGES.md")


class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest

        errcode = pytest.main(self.test_args)
        sys.exit(errcode)


setup(
    name="photon-pump",
    version=versioneer.get_version(),
    url="http://github.com/madedotcom/photon-pump/",
    license="MIT",
    author="Bob Gregory",
    install_requires=["aiodns>=1.1.1", "aiohttp>=3.1.3", "protobuf>=3.5.1"],
    cmdclass=versioneer.get_cmdclass(),
    author_email="bob@made.com",
    description="Fast, easy to use client for EventStore",
    long_description=long_description,
    packages=["photonpump"],
    include_package_data=True,
    platforms="any",
    test_suite="photonpump.test",
    classifiers=[
        "Programming Language :: Python",
        "Development Status :: 3 - Alpha",
        "Natural Language :: English",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Software Development :: Libraries :: Application Frameworks",
    ],
    extras_require={"testing": ["pytest"]},
)
