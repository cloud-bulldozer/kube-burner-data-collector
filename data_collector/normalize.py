import re
import logging
import numpy as np
import pandas as pd
from typing import Dict, List
from data_collector.utils import (
    strhash,
    should_exclude,
    compile_exclude_patterns,
    recursively_flatten_values,
    remove_keys_by_patterns,
    flatten_json,
)

logger = logging.getLogger(__name__)

DROP_LIST = ['metadata','uuid','metricName','labels','query', 'value', 'jobName', 'timestamp']
NEST_ORDER = ["mode", "scope", "verb", "namespace", "component", "resource", "container", "endpoint"]
DEFAULT_HASH = "xyz"


def process_json(metric: str, entries: dict, skip_patterns: List[re.Pattern], output: Dict) -> None:
    """Processes JSON and generates a huge json with minimal data"""
    if not entries:
        return

    metric_name = entries[0].get("metricName")
    if not metric_name:
        logger.info(f"Warning: 'metricName' missing in first entry of the metric: {metric}")
        return

    if should_exclude(metric_name, skip_patterns):
        return

    grouped_metrics = {}
    for entry in entries:
        # Skip the metircs during churn phase to avoid noise
        if 'churnMetric' in entry:
            continue
        # Skip metrics during garbage collection as well to avoid noise
        if 'jobName' in entry and entry['jobName'].lower() == 'garbage-collection':
            continue
        label_hash = DEFAULT_HASH
        labels = entry.get("labels")
        if labels:
            label_hash = strhash(labels)

        if label_hash not in grouped_metrics:
            grouped_metrics[label_hash] = {"value": 0.0}
            if labels:
                grouped_metrics[label_hash]["labels"] = {k: labels[k] for k in NEST_ORDER if k in labels}

        # Drop unneeded fields
        if "value" in entry:
            # reduces value to average
            entry = {"value": entry["value"]}
            grouped_metrics[label_hash]["value"] += entry["value"]/2
        else:
            # handles cases where metrics don't have value. for example, quantiles
            for k in DROP_LIST:
                entry.pop(k,None)
            # Need to deal with this edge case as we set {"value": 0.0} as default above
            if isinstance(grouped_metrics[label_hash]["value"], (int, float)):
                grouped_metrics[label_hash].pop("value", None)
            if "value" not in grouped_metrics[label_hash]:
                grouped_metrics[label_hash]["value"] = [entry]
            else:
                grouped_metrics[label_hash]["value"].append(entry)

    # Adds up condensed data values to output json
    if metric_name in output["metrics"]:
        output["metrics"][metric_name].extend(grouped_metrics.values())
    else:
        output["metrics"][metric_name] = list(grouped_metrics.values())

def normalize_metrics(metrics: dict) -> dict:
    """Intermidiate normalization step to further reduce the json"""

    # Labels precedence order used for nesting
    nested_metrics = {}

    for metric, entries in metrics:
        nested_metrics.setdefault(metric, {})

        for entry in entries:
            labels = entry.get("labels", {})
            value = entry["value"]

            # Get available keys from labels, in nest_order
            label_keys = [k for k in NEST_ORDER if k in labels]
            if not label_keys:
                # No labels at all, store directly under metric
                existing = nested_metrics[metric]
                if isinstance(existing, (int, float)):
                    nested_metrics[metric] = (existing + value) / 2
                elif isinstance(existing, dict):
                    if "_value" in nested_metrics[metric]:
                        nested_metrics[metric]["_value"] = (nested_metrics[metric]["_value"] + value) / 2
                    else:
                        nested_metrics[metric]["_value"] = value
                else:
                    nested_metrics[metric] = value
                continue

            curr = nested_metrics[metric]
            for _, key in enumerate(label_keys):
                # logc to generate nested keys with labels
                key_value = labels[key]
                group_key = f"byLabel{key.capitalize()}"
                curr = curr.setdefault(group_key, {})
                if key_value in curr:
                    if isinstance(curr[key_value], (int, float)):
                        curr[key_value] = {"_value": curr[key_value]}
                    curr = curr[key_value]
                else:
                    curr = curr.setdefault(key_value, {})

            # Now we're at the leaf, insert _value
            if "_value" in curr:
                curr["_value"] = (curr["_value"] + value) / 2
            else:
                curr["_value"] = value

    return nested_metrics

def get_cluster_health(alerts: list, passed: bool) -> str:
    """Calculates and returns cluster health"""
    has_error, has_warning = False, False
    for alert in alerts:
        if alert["severity"].lower() == 'warning':
            has_warning = True
        if alert["severity"].lower() == 'error':
            has_error = True
    if has_error or not passed:
        return "Red"
    if has_warning:
        return "Yellow"
    return "Green"

def normalize(metrics_data, data_filters, extract_filters, fields_to_reduce: dict, exclude_metrics: str):
    """Driver code to triger the execution"""
    skip_patterns = compile_exclude_patterns(exclude_metrics)

    merged_output = {"metrics": {}}

    for metric, value in metrics_data["metrics"].items():
        process_json(metric, value, skip_patterns, merged_output)

    nested_metrics = normalize_metrics(merged_output["metrics"].items())

    final_output = recursively_flatten_values(nested_metrics)

    flattened = {}
    flatten_json(flattened, final_output)
    patterns_to_remove = [r"(?i).*time.*", r"uuid", r"version"]
    metadata = remove_keys_by_patterns(metrics_data["metadata"], patterns_to_remove)
    for key, value in metadata.items():
        if "jobConfig" != key:
            flattened[key] = value
        else:
            for key, value in metadata.get("jobConfig", {}).items():
                flattened[f"jobConfig.{key}"] = value

    # Filter rows by data filters (e.g., platform == AWS)
    has_atleast_one_filter = False
    if data_filters:
        for filter in data_filters:
            key, value = list(filter.items())[0]
            if flattened.get(key) ==  value:
                has_atleast_one_filter = True
                break
    if not has_atleast_one_filter:
        return {}

    # Extract matching fields (based on regex)
    fields_to_keep = set()
    fields_with_prefix = set()
    if extract_filters:
        for extract_filter in extract_filters:
            key, value = list(extract_filter.items())[0]
            key_pattern = re.compile(key)
            value_pattern = re.compile(value)
            for field in flattened.keys():
                if key_pattern.match(field):
                    fields_with_prefix.add(field)
                    if value_pattern.match(field):
                        fields_to_keep.add(field)
    flattened = {k: v for k, v in flattened.items() if k not in fields_with_prefix - fields_to_keep}

    # Reduce multiple fields into one target (based on regex)
    if fields_to_reduce:
        for field in fields_to_reduce:
            key, target_key = list(field.items())[0]

            # Find all matching keys
            matching_items = {k: v for k, v in flattened.items() if re.match(key, k)}

            if not matching_items:
                continue

            # Collect valid values
            values = [v for v in matching_items.values() if v is not None and str(v) != "nan"]
            if not values:
                flattened[target_key] = None
            else:
                try:
                    numeric_vals = pd.to_numeric(values, errors="coerce").dropna()
                    if len(numeric_vals) > 0:
                        flattened[target_key] = float(np.mean(numeric_vals))  # average
                    else:
                        flattened[target_key] = pd.Series(values).median()  # median
                except Exception:
                    flattened[target_key] = pd.Series(values).median()

            # Drop the original matching keys, since we replaced them
            for k in matching_items.keys():
                if k != target_key:  # avoid removing the reduced one
                    flattened.pop(k, None)

    alerts = metrics_data["metrics"]["alert"] if 'alert' in metrics_data["metrics"] else []
    flattened["cluster_health_score"] = get_cluster_health(alerts, metadata["passed"])
    return flattened
