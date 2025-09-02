import logging
from typing import Dict
from data_collector.utils import (
    recursively_flatten_dict
)
import pandas as pd

logger = logging.getLogger(__name__)

def get_cluster_health(passed: bool, execution_errors: str) -> str:
    """Calculates and returns cluster health"""
    if not passed:
        return "Red"
    if execution_errors:
        return "Yellow"
    return "Green"

def normalize(metrics_data: Dict, config: Dict) -> pd.DataFrame:
    """
    Normalize data from a job run.

    This function takes the metrics data from a job run and normalizes it into a
    pandas DataFrame. It also adds the cluster health score to the resulting
    DataFrame.

    Args:
        metrics_data (Dict): The data from a job run.
        config (Dict): The configuration for the job.

    Returns:
        pd.DataFrame: The normalized DataFrame.
    """
    run_df = pd.DataFrame()
    metadata = metrics_data.pop("metadata", {})
    job_config = metadata.pop("jobConfig", {})
    for metric_name, metric_samples in metrics_data["metrics"].items():
        for metric in config["metrics"]:
            if metric["name"] == metric_name:
                metric_config = metric
        aggregated_metric_samples = {}
        for metric_sample in metric_samples:
            if "value_field" in metric_config:
                metric_sample["value"] = metric_sample.pop(metric_config["value_field"])
            metric_sample = recursively_flatten_dict(metric_sample)
            # metadata and job config must be added to the flattened and averaged result
            for field in config.get("discard_fields", []):
                metric_sample.pop(field, None)
            if not aggregated_metric_samples:
                aggregated_metric_samples = metric_sample
            else:
                aggregated_metric_samples["value"] += metric_sample["value"] / 2
        aggregated_metric_samples.update(metadata)
        aggregated_metric_samples.update(job_config)
        aggregated_metric_samples["cluster_health_score"] = get_cluster_health(aggregated_metric_samples["passed"], aggregated_metric_samples.pop("execution_errors", ""))
        aggregated_metric_samples["description"] = metric_config.get("description", "")
        # We convert the list of dictionaries into a DataFrame
        df = pd.DataFrame([aggregated_metric_samples])
        run_df = pd.concat([run_df, df], ignore_index=True)
    return run_df
