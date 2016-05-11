# Elasticsearch Utils

Mainly because I was fed up exporting and bulk inserting data into ES, a simple utility script that works on ES `2.3.2` with shield plugin enabled.
Download index data as batch (using `scrolling` queries) where each batch can be re-indexed using `Bulk` load or exported to a "bulk-ready" file.

## Conf

Basic configuration in src/main/resources/application.conf

- `host`: the hostname of elasticsearch agent
- `port`: the transport port to use (9300)
- `cluster`: the cluster name to join
- `timeValue`: the time value for scrolling query (60000)
- `batchSize`: the batch size of each scroll (100)
- `username`: the shield username (ignored if shield disabled)
- `password`: the shield password (ignored if shield disabled)

Should you need to override configuration file at runtime

```sh
java -Dconfig.file=/path/to/new/application.conf -jar elasticsearch-utils.jar
```

## Usage

```sh
java -jar elasticsearch-utils.jar --help
usage: Utils
 -f,--output-file <arg>    The output file to write bulk data to
 -h,--help                 Prints help menu
 -i,--input-index <arg>    The input index/type to read data
 -o,--output-index <arg>   The output index/type to re-index data to
 -w,--write                Write back to elasticsearch
```

Both the input and output indices (name/type) are required. 

If `--output-file` option enabled, each batch is appended to the given output file using below "bulk-ready" format using the original document Id

```json
{"index":{"_index":"index2", "_type":"type2", "_id": "1"}}
{"Title":"Product 1", "Description":"Product 1 Description"}
{"index":{"_index":"index2", "_type":"type2", "_id":"2"}}
{"Title":"Product 2", "Description":"Product 2 Description"}
```

```sh
java -jar elasticsearch-utils.jar --input-index index1/type1 --output-index index2/type2 --output-file /tmp/export.json
```

Output file can easily be re-imported on a different cluster using `_bulk` POST query.

```sh
curl -s -XPOST localhost:9200/_bulk --data-binary "@/tmp/export.json"
```

If `--write` option is enabled, each batch goes straight to the provided index name/type of the **same elasticsearch instance** (useful for re-indexing data)

```sh
java -jar elasticsearch-utils.jar --input-index index1/type1 --output-index index2/type2 -w
```

## Build

```
mvn clean package
```

## Version

1.0

## Author

Antoine Amend