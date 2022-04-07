import pathlib

import setuptools

HERE = pathlib.Path(__file__).parent
README = (HERE / "README.md").read_text(encoding="UTF-8")


setuptools.setup(
    name="e4psycopg",
    version="1.0.0",
    description="Generate SQL statements to use with psycopg.",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/midir99/e4psycopg",
    author="midir99",
    author_email="midir99@protonmail.com",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
    packages=setuptools.find_packages("src"),
    package_dir={"": "src"},
    include_package_data=True,
    install_requires=["feedparser", "html2text"],
    entry_points={
        "console_scripts": [
            "realpython=e4psycopg.cli:main",
        ]
    },
)
