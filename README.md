# Kube-burner Data Collector

Kube-burner data-collector

-  Free software: Apache Software License 2.0

## Installation

```shell
pip install -r requirements.txt
python setup.py install
```

## Running

```
$ data_collector -es-server 'https://elastic-search-fqdn' --es-index 'kube-burner*' --config config/metrics.yml --from $(date -d "2 months ago" +%s)
```

## Configuration

A configuration file is stored at [metrics.yml](config/metrics.yml). And has the following directives:

- `metadata`: List of metadata fields to process from the fetched job summaries.
- `job_summary_filters`: List of filters to apply to the job summaries query.
- `metrics`: List of metrics (data-collector uses the job summaries UUIDs to pull these metrics) to normalize
  - `name`: Name of the metric, i.e: `metricName.keyword: metric_name_in_config`
  - `value_field`: Name of the field that contains the value, defaults to `value`
  - `description`: Description of the metric, will be added to the normalized output
- `skip_metrics`: Metrics with the following fields will be skipped:
- `discard_fields`: List of fields to discard from normalized output.
- `output_prefix`: Prefix for the output file
- `s3_bucket`: Name of the S3 bucket
- `s3_folder`: Name of the S3 folder
- `chunk_size`: Size of the chunks (number of lines) to upload to S3
