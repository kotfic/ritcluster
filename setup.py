from setuptools import setup

setup(
    name="ritcluster",
    version="0.0",
    author="Christoper Kotfila",
    author_email="kotfic@gmail.com",
    license="GPL",
    py_modules=['ritcluster'],
    entry_points={
        "console_scripts": [
            "cluster = ritcluster:main"
        ]
    }
)

