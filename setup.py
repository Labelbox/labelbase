import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
      name='labelbase',
      version='0.0.45',
      author='Labelbox',
      author_email='raphael@labelbox.com',
      description='Labelbox Helper Library',      
      packages=setuptools.find_packages(),
      url="https://labelbox.com",
      long_description=long_description,
      long_description_content_type="text/markdown",
      install_requires=["labelbox", "packaging"],
      keywords=["labelbox", "labelbase"],
      extras_require={'dev': ['pylint']}
)
