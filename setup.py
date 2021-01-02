from setuptools import setup


setup(
    name="bgipython",
    description="Run jobs in background in ipython.",
    long_description=open("README.rst", encoding="utf-8").read(),
    author="Dror Speiser",
    url="https://github.com/drorspei/bgipython",
    license="MIT",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Framework :: IPython",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
    ],
    py_modules=["bgipython"],
    package_dir={"": "bgipython"},
    python_requires=">=3.7",
    install_requires=[
        "ipython>=7",  # IPython#8985 is needed for tests to pass(?).
    ],
)
