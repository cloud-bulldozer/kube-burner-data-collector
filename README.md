# Kube-burner Data Collector

Kube-burner data-collector

-  Free software: Apache Software License 2.0

## Installation

```shell
pip install -r requirements.txt
python setup.py install
```

## Running

It can be run from the command line as:

```shell
$ data_collector --es-server 'https://elastic-search-fqdn' --es-index 'kube-burner*' --config config/metrics.yml --from $(date -d "2 months ago" +%s)
```

## Configuration

A configuration file is stored at [metrics.yml](config/metrics.yml). And has the following directives:

- `output_prefix`: Prefix for the output file
- `s3_bucket`: Name of the S3 bucket
- `s3_folder`: Name of the S3 folder
- `chunk_size`: Size of the chunks (number of lines) to upload to S3