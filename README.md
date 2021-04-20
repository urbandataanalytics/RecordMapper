# RecordMapper
Read, transform and write records using an Avro Schema and custom map functions.

## Installing the project

To install the project, move to the root directory and run the following command:

```bash
$pip install .
```

It is highly recommended to use a virtual environment when installing the project dependencies.

## Update PyPI version

    poetry publish --build
 
## Appliers

## Readers

To apply the record transformations, the Record Mapper must be able to read the file that
contains the data in order to extract them. The Record Mapper supports reading files from 
different formats, including csv, xml and avro.

The reading process is done by the *Reader* objects. A Reader class implements methods to 
read different kind of files. The Reader class is extended by sub-classes, each of them 
specialized in reading an specific format. For example, to read an XML file we can use the
XMLReader class. To extract data from a CSV file, we will be using the CSVReader class.

A Reader sub-class implements the *read_records_from_input* method, which will return the
content of the file **record by record**.

Each specific Reader sub-class is located in the directory of its own format, sharing space
with the correspondent Writer sub-class (we will be talking about it soon).


## Writers

After applying the record transformations, the Record Mapper must be able to write the
resultant transformed records in a file. It supports writing files for different formats, 
including csv and avro. 

Important! The Record Mapper can return different files as output, one for each format, 
but at least it is mandatory to write the avro file. Thus, avro file is always returned.

The writing process is done by the *Writer* objects. A Writer class implements methods to 
write to different formats. The Writer class is extended by sub-classes, each of them 
specialized in writing an specific format. For example, to write a CSV file we can use the
CSVWriter class while we will be using the AvroWriter to write an Avro file.

A Writer sub-class implements the *write_records_to_output* method, which will write a given
iterable of records in an output file. The method accepts other output options as parameters.
These output options include:
- Flattening nested schemas when writing csv files.
- Merging schemas when writing avro files.

<TODO: Review this.>

Each specific Writer sub-class is located in the directory of its own format, sharing space
with the correspondent Reader sub-class.
