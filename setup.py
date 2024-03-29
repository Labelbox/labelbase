import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
      name='labelbase',
      version='0.1.05',
      author='Labelbox',
      author_email='raphael@labelbox.com',
      description='Labelbox Helper Library',      
      packages=setuptools.find_packages(),
      url="https://labelbox.com",
      long_description=long_description,
      long_description_content_type="text/markdown",
      install_requires=["labelbox[data]", "packaging"],
      keywords=["labelbox", "labelbase"],
      extras_require={'dev': ['pylint']}
)
