from setuptools import setup, find_packages

setup(
    name="PipelineInfrastructure",
    version="0.0.11",
    python_requires='>=3.6.0',
    url="https://github.com/AfricasVoices/Pipeline-Infrastructure",
    packages=find_packages(exclude=("test",)),
    install_requires=["firebase_admin", "google-cloud-firestore", "google-cloud-storage", "google-api-python-client",
                      "oauth2client", "coredatamodules"]
)
