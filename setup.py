from setuptools import setup, find_packages

setup(
    name = 'pyelasticsearch-tornado',
    version = '0.6.1',
    author = "Brian Lee",
    description = "Basic pyelasticsearch wrapper using Tornado's AsyncHTTPClient",
    long_description = (
        "This wrapper is simply intended to allow pyelasticsearch to be used inside Tornado's event loop "
        "without blocking and without having to use Tornado's ThreadPoolExecutor. This wrapper may be "
        "missing features or out of date, and is designed to be as simple as possible for use with "
        "ElasticSearch's REST API."
    ),
    license = "BSD",
    keywords = "tornado pyelasticsearch elasticsearch async http rest",
    install_requires = ["tornado >= 4.0", "pyelasticsearch == 0.6.1", "six"],
    packages = find_packages(),
)
