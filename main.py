"""Console script for data_collector."""

import os
import sys
import logging
import argparse
import urllib3
import csv
import pandas as pd
from data_collector import __version__, collector
from data_collector.config import Config
from data_collector.normalize import normalize
from data_collector import output
from data_collector.utils import split_list_into_chunks, parse_timerange
from data_collector.constants import VALID_LOG_LEVELS
from data_collector.logging import configure_logging
import datetime

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def main():
    """Console script for data_collector."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("--log-level", 
                        type=str, 
                        choices=VALID_LOG_LEVELS, 
                        default=os.environ.get("LOG_LEVEL", "INFO").upper(), 
                        help="Logging level (e.g., DEBUG, INFO, WARNING, ERROR, CRITICAL). Can also be set via LOG_LEVEL env var"
    )
    parser.add_argument("--es-server", action="store", help="ES Server endpoint", required=True)
    parser.add_argument("--es-index", action="store", help="ES Index name", required=True)
    parser.add_argument("--config", action="store", help="Configuration file")
    parser.add_argument(
        "--from",
        action="store",
        help="Start date, in epoch seconds",
        required=True,
        type=int,
        dest="from_date",
    )
    parser.add_argument(
        "--to",
        action="store",
        help="End date, in epoch seconds",
        type=int,
        default=datetime.datetime.now(datetime.UTC).timestamp(),
    )
    parser.add_argument(
        "--output",
        action="store",
        help="Output type",
        choices=["s3", "file"],
        type=str,
        default="s3",
    )
    args = parser.parse_args()
    configure_logging(args.log_level)
    logger = logging.getLogger(__name__)
    logger.info(f"CLI args: {args}")
    from_date, to = parse_timerange(args.from_date, args.to)
    config = Config(args.config)
    logger.debug(f"Processing input configuration: {config}")
    input_config = config.parse()
    collector_instance = collector.Collector(args.es_server, args.es_index, input_config)
    data = collector_instance.collect(from_date, to)
    df = pd.DataFrame()
    for each_run in data:
        for uuid, run_json in each_run.items():
            uuid_df = normalize(run_json, input_config)
            df = pd.concat([df, uuid_df], ignore_index=True)
    if not df.empty:
        for idx, chunk in enumerate(split_list_into_chunks(df, input_config["chunk_size"]), start=1):
            filename = f"{input_config['output_prefix']}_{from_date.strftime('%Y-%m-%dT%H:%M:%SZ')}_{to.strftime('%Y-%m-%dT%H:%M:%SZ')}_chunk_{idx}.csv"
            if args.output == "s3":
                output.upload_csv_to_s3(chunk, input_config["s3_bucket"], input_config["s3_folder"], filename)
            else:
                output.write_to_file(chunk, filename)
    return 0

if __name__ == "__main__":
    sys.exit(main())
