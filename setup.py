from setuptools import setup, find_packages

setup(
    name="meta_search",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pandas>=1.3.0",
        "numpy>=1.20.0",
        "scikit-learn>=1.0.0",
        "faiss-cpu>=1.7.0",
        "tqdm>=4.60.0",
    ],
    author="Your Name",
    description="A flexible search system for job data",
)