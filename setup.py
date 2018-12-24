from setuptools import setup, find_packages

setup(
    name="PipelineInfrastructure",
    version="0.0.1",
    url="https://github.com/AfricasVoices/Pipeline-Infrastructure",
    
    packages=find_packages(exclude=('test',)),
    
    install_requires=[],
    python_requires='>=3.6.0'
)
