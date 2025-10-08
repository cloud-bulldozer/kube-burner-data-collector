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
