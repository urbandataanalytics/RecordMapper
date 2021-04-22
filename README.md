# RecordMapper

Read, transform and write records using an Avro schema and custom map functions.


## Installing the project

To install the project, run the following command from the root directory:

```bash
$pip install .
```

It is highly recommended to use a virtual environment when installing the project 
dependencies in order to avoid version conflicts.


## Updating PyPI version

    poetry publish --build
 
 
## RecordMapper elements
 
### Appliers

Appliers are the elements that materialise the records transformations. They apply 
sequentially specific transformations to each record and/or its schema. 

There are four appliers defined by now:

- The selector applier, which will modify the base schema if there exist nested schemas to consider.
- The rename applier, which will develop a renaming process using the aliases included in the schema.
- The transform applier which will apply the record transformations given the transforming functions.
- The clean applier, which will filter the output fields to keep only the ones given in the output schema.

An Applier is a class that implements the *apply* method, which receives a single 
record and its schema and returns their transformed version after the transforming
process. They are located in the appliers directory.


### Readers

To apply the records transformations, the Record Mapper must be able to read the file that
contains the data in order to extract them. The Record Mapper supports reading files from 
different formats, including csv, xml and avro.

The reading process is done by the *Reader* objects. A Reader class implements methods to 
read different kind of files. The Reader class is extended by sub-classes, each of them 
specialized in reading an specific format. For example, to read an XML file we can use the
XMLReader class. To extract data from a CSV file, we will be using the CSVReader class.

A Reader sub-class implements the *read_records_from_input* method, which will return the
content of the file record by record. Each specific Reader sub-class is located in the 
directory of its own format, sharing space with the correspondent Writer sub-class.


### Writers

After applying the records transformations, the Record Mapper must be able to write the
resultant transformed records in a file. It supports writing files for different formats, 
including csv and avro. 

Important! The Record Mapper can return different files as output, one for each format, 
but at least it is mandatory to write the avro file. Thus, avro file is always returned.

The writing process is done by the *Writer* objects. A Writer class implements methods to 
write to different formats. The Writer class is extended by sub-classes, each of them 
specialized in writing an specific format. For example, to write a CSV file we can use the
CSVWriter class while we will be using the AvroWriter to write an Avro file.

A Writer sub-class implements the *write_records_to_output* method, which will write a given
iterable of records in an output file. Each specific Writer sub-class is located in the 
directory of its own format, sharing space with the correspondent Reader sub-class. The 
method accepts other output options as parameters. These output options include:
- Flattening nested schemas when writing csv files.
- Merging schemas when writing avro files.
