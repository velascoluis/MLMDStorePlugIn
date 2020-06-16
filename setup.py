from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='mlflow-mldmstore',
    version='0.1',
    description='Plugin that provides MLMD Tracking Store functionality for MLflow',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='Luis Velasco',
    author_email='velascoluis@google.com',
    url="https://github.com/velascoluis",
    packages=find_packages(),
    install_requires=[
        'mlflow', 'kubeflow-metadata'
    ],
    entry_points={
        "mlflow.tracking_store": [
            "http=mlmdstore.store.tracking.mlmd_store:MLMDStore"
        ]
    },
)


