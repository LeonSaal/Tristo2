import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="tristo",
    version="v2.2",
    author="Leon Saal",
    author_email="leon.saal@uba.de",
    description="A package for gathering drinking water quality data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/LeonSaal/Tristo2",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3.0",
        "Operating System :: OS Independent",
    ],
    packages=setuptools.find_packages(),
    install_requires=[
        "pandas",
        "openpyxl",
        "requests",
        "pint",
        "selenium",
        "PyMuPDF",
        "tabula-py",
        "camelot-py",
        "ocrmypdf",
        "SQLAlchemy",
        "beautifulsoup4",
        "matplotlib",
        "ipykernel",
        "lxml",
        "numpy",
        "scipy",
        "opencv-python",
        "wget",
        "thefuzz",
    ],
    python_requires=">=3.10.4",
)
