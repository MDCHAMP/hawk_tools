from setuptools import setup, find_packages

setup(
    name='hawk_tools',
    version='0.1',
    url='',
    author='MDCHAMP',
    author_email='max.champneys@sheffield.ac.uk',
    description='Tooling for the INI deep dive with the HAWK data',
    packages=find_packages(),    
    include_package_data=True,
    install_requires=['six', 'gdown','h5py'],
)