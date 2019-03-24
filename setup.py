import setuptools


with open("README.md", "r") as fh:
    long_description = fh.read()


setuptools.setup(
    name="apimux",
    version="0.1",
    author="Alin Balutoiu",
    description="API Multiplexer package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/alinbalutoiu/apimux",
    packages=setuptools.find_packages(),
)
