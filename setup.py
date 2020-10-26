import setuptools

DEPENDENCIES = [
    'coverage==4.5.4',
    'nose==1.3.7',
    'fastavro==0.23.4'
]

with open('README.md', encoding='utf-8') as f:
    long_description = f.read()

setuptools.setup(
    name='RecordMapper',
    version='0.1.0',
    description='Transform records using an Avro Schema and custom map functions',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='UDARealState Data engineering Team',
    url='https://github.com/urbandataanalytics/RecordMapper',
    install_requires=DEPENDENCIES,
    packages=setuptools.find_packages(),
    test_suite="nose.collector",
    python_requires=">=3.6",
    classifiers=[
        "License :: OSI Approved :: MIT License"
    ]
)
